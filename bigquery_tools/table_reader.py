#!/usr/bin/python2.7

# Adopted from code provided in https://code.google.com/archive/p/bigquery-e2e/

__author__ = 'Paulius Danenas'

from apiclient.errors import HttpError
from auth import BigQuery_Auth
from argparse import ArgumentParser
from datetime import datetime
from progressbar import Percentage, Bar, ProgressBar, Timer
import logging
import os
import sys
import threading
import time
from output_handler import FileResultHandler, CSVResultHandler, JSONResultHandler

READ_CHUNK_SIZE = 64 * 1024

class TableReader:
    '''Reads data from a BigQuery table.'''

    def __init__(self, auth, project_id, dataset_id, table_id,
                 start_index=None, read_count=None, next_page_token=None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_service = auth.build_bq_client()
        self.next_page_token = next_page_token
        self.next_index = start_index
        self.rows_left = read_count
        self.table_id = table_id
        self.auth = auth

    def get_table_info(self):
        '''Returns core information for the table.'''
        table = self.bq_service.tables().get(projectId=self.project_id,
                                             datasetId=self.dataset_id,
                                             tableId=self.table_id).execute()
        last_modified = int(table.get('lastModifiedTime', 0))
        last_modified = datetime.fromtimestamp(int(last_modified / 1000))
        row_count = int(table.get('numRows', 0))
        columns = [field['name'] for field in table['schema']['fields']]
        column_types = {field['name']: field['type'] for field in table['schema']['fields']}
        print '%s last modified at %s' % (table['id'], last_modified.strftime("%b %d %Y %H:%M:%S"))
        return (last_modified, row_count, columns, column_types)

    def advance(self, rows, page_token):
        '''Called after reading a page, advances current indices.'''
        done = page_token is None
        if self.rows_left is not None:
            self.rows_left -= len(rows)
            if self.rows_left < 0: print 'Error: Read too many rows!'
            if self.rows_left <= 0: done = True

        if self.next_index is not None:
            self.next_index += len(rows)
        else:
            # Only use page tokens when we're not using index-based pagination.
            self.next_page_token = page_token
        return done

    def get_table_id(self):
        if '@' in self.table_id and self.snapshot_time is not None:
            raise Exception("Table already has a snapshot time")
        if self.snapshot_time is None:
            return self.table_id
        else:
            return '%s@%d' % (self.table_id, self.snapshot_time)

    def make_read_message(self, row_count, max_results):
        '''Creates a status message for the current read operation.'''
        read_msg = 'Read %d rows' % (row_count,)
        if self.next_index is not None:
            read_msg = '%s at %s' % (read_msg, self.next_index)
        elif self.next_page_token is not None:
            read_msg = '%s at %s' % (read_msg, self.next_page_token)
        else:
            read_msg = '%s from start' % (read_msg,)
        if max_results <> row_count:
            read_msg = '%s [max %d]' % (read_msg, max_results)
        return read_msg

    def read_one_page(self, max_results=READ_CHUNK_SIZE):
        '''Reads one page from the table.'''
        while True:
            try:
                if self.rows_left is not None and self.rows_left < max_results:
                    max_results = self.rows_left
                data = self.bq_service.tabledata().list(
                    projectId=self.project_id,
                    datasetId=self.dataset_id,
                    tableId=self.get_table_id(),
                    startIndex=self.next_index,
                    pageToken=self.next_page_token,
                    maxResults=max_results).execute()
                next_page_token = data.get('pageToken', None)
                rows = data.get('rows', [])
                print self.make_read_message(len(rows), max_results)
                is_done = self.advance(rows, next_page_token)
                return (is_done, rows)
            except HttpError, err:
                # If the error is a rate limit or connection error, wait and
                # try again.
                if err.resp.status in [403, 500, 503]:
                    print '%s: Retryable error %s, waiting' % (
                        self.thread_id, err.resp.status,)
                    time.sleep(5)
                else:
                    raise

    def read(self, result_handler, snapshot_time=None):
        '''Reads an entire table until the end or we hit a row limit.'''
        # Read the current time and use that for the snapshot time.
        # This will prevent us from getting inconsistent results when the
        # underlying table is changing.
        _, row_count, _, _ = self.get_table_info()
        if snapshot_time is None and not '@' in self.table_id:
            self.snapshot_time = int(time.time() * 1000)
        self.snapshot_time = snapshot_time
        pbar = ProgressBar(widgets=[Percentage(), Bar(), Timer()], maxval=row_count).start()
        while True:
            is_done, rows = self.read_one_page()
            if rows:
                result_handler.handle_rows(rows)
                pbar.update(len(rows))
            if is_done:
                result_handler.finish()
                pbar.finish()
                return

    def parallel_indexed_read(self, partition_count, output_dir, output_format='csv', sep=';'):
        '''Divides up a table and reads the pieces in parallel by index.'''
        _, row_count, _, _ = self.get_table_info()
        snapshot_time = int(time.time() * 1000)
        stride = row_count / partition_count
        threads = []
        for index in range(partition_count):
            if not (os.path.exists(output_dir) and os.path.isdir(output_dir)):
                os.makedirs(output_dir)
            file_name = '%s.%d' % (os.path.join(output_dir, self.table_id), index)
            start_index = stride * index
            thread_reader = TableReader(auth=self.auth, project_id=self.project_id,
                                        dataset_id=self.dataset_id,
                                        table_id='%s@%d' % (self.table_id, snapshot_time),
                                        start_index=start_index, read_count=stride)
            read_thread = TableReadThread(thread_reader, file_name,
                                          thread_id='[%d-%d)' % (start_index, start_index + stride),
                                          output_format=output_format, sep=sep)
            threads.append(read_thread)
            threads[index].start()
        for index in range(partition_count):
            threads[index].join()

    def parallel_partitioned_read(self, partition_count, output_dir, output_format='csv', sep=';'):
        ''' Table must be partitioned to use this technique! '''
        snapshot_time = int(time.time() * 1000)
        threads = []
        for index in range(partition_count):
            file_name = '%s.%d' % (os.path.join(output_dir, self.table_id), index)
            suffix = '%d-of-%d' % (index, partition_count)
            partition_table_id = '%s@%d%s' % (self.table_id, snapshot_time, suffix)
            thread_reader = TableReader(auth=self.auth, project_id=self.project_id,
                dataset_id=self.dataset_id, table_id=partition_table_id)
            read_thread = TableReadThread(thread_reader, file_name, thread_id=suffix,
                                          output_format=output_format, sep=sep)
            threads.append(read_thread)
            threads[index].start()
        for index in range(partition_count):
            threads[index].join()


class TableReadThread(threading.Thread):
    '''Thread that reads from a table and writes it to a file.'''

    def __init__(self, table_reader, output_file_name,
                 thread_id='thread', output_format='csv', sep=';'):
        threading.Thread.__init__(self)
        self.table_reader = table_reader
        self.output_file_name = output_file_name
        self.thread_id = thread_id
        self.output_format = output_format
        self.sep = sep

    def get_columns(self):
        _, _, columns, _ = self.table_reader.get_table_info()
        return columns

    def get_result_handler(self):
        if self.output_format.lower() == 'csv':
            return CSVResultHandler(self.output_file_name, columns=self.get_columns(), sep=self.sep)
        elif self.output_format.lower() == 'json':
            return JSONResultHandler(self.output_file_name)
        else:
            return FileResultHandler(self.output_file_name)

    def run(self):
        print 'Reading %s' % (self.thread_id,)
        self.table_reader.read(self.get_result_handler())


def main(argv):
    logging.basicConfig()
    parser = ArgumentParser(description='Read BigQuery table into a text file')
    parser.add_argument('-a', '--service_account', required=True, help='Big Query service account name')
    parser.add_argument('-s', '--client_secret', required=True,
                        help='Path to client_secrets.json file required for API login')
    parser.add_argument('-c', '--credentials',
                        help="""Path to credentials file (e.g. bigquery_credentials.dat) required for API login.
                        If the file is not present, the browser window will be shown and you will be asked to authenticate
                        """)
    parser.add_argument('-k', '--keyfile', help='Path to the key file (e.g., key.p12)')
    parser.add_argument('-p', '--project_id', required=True, help="BigQuery project ID")
    parser.add_argument('-d', '--dataset_id', required=True, help="The name of the BigQuery dataset which contains the table")
    parser.add_argument('-t', '--table_id', required=True, help='Name of the table which will be exported')
    parser.add_argument('-o', '--output_directory', default='.', help='The directory where the output will be exported')
    parser.add_argument('-f', '--format', default='json', choices=['json', 'csv'], help='The output format')
    parser.add_argument('--separator', help='Separator in CSV', default=';')
    parser.add_argument('--type', choices=['single-thread', 'parallel-indexed', 'parallel-partitioned'],
                        default='single-thread', help='Reader type')
    parser.add_argument('--partition_count', type=int, default=10, help='Number of partitions for parallel reading')
    args = parser.parse_args()

    auth = BigQuery_Auth(service_acc=args.service_account, client_secrets=args.client_secret,
                         credentials=args.credentials, key_file=args.keyfile)
    table_reader = TableReader(auth, project_id=args.project_id,
                               dataset_id=args.dataset_id, table_id=args.table_id)
    fname = table_reader.table_id + '.' + args.format if args.format is not None else table_reader.table_id
    output_file_name = os.path.join(args.output_directory, fname)
    if args.type == 'single-thread':
        thread = TableReadThread(table_reader, output_file_name,
                                 output_format=args.format, sep=args.separator)
        thread.start()
        thread.join()
    elif args.type == 'parallel-indexed':
        table_reader.parallel_indexed_read(output_dir=args.output_directory,
                                           partition_count=args.partition_count,
                                           output_format=args.format,
                                           sep=args.separator)
    elif args.type == 'parallel-partitioned':
        table_reader.parallel_partitioned_read(output_dir=args.output_directory,
                                               partition_count=args.partition_count,
                                               output_format=args.format,
                                               sep=args.separator)


if __name__ == "__main__":
    main(sys.argv[1:])
