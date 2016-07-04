#!/usr/bin/python2.7

# Adopted from code provided in https://code.google.com/archive/p/bigquery-e2e/

__author__ = 'Paulius Danenas'

'''Exports a BigQuery table to Google Cloud Storage.

This module runs a BigQuery Extract Job to export a table
source_project_id:source_dataset_id.source_table_id
to the Google Cloud Storage location specified by under the
destination_bucket. It supports simple and partitioned reading modes
If partitioned mode is used, the job  will instruct BigQuery to extract
in partition_count partitions, and it will read those partitioned
files in parallel threads.
If destination_dir is specified, will download the results to that
directory, otherwise will just report the presence of the output files
in GCS.

The extract job will run in the project specified by project_id.
'''

import sys
import threading
import time
import logging
import json
from argparse import ArgumentParser
from gcs_reader import GcsReader
from job_runner import JobRunner
from auth import BigQuery_Auth


class SimpleReader:

    def make_extract_config(self, source_project_id, source_dataset_id,
                            source_table_id, destination_uris):
        '''Creates a dict containing an export job configuration.'''
        source_table_ref = {
            'projectId': source_project_id,
            'datasetId': source_dataset_id,
            'tableId': source_table_id}
        extract_config = {
            'sourceTable': source_table_ref,
            'destinationFormat': 'NEWLINE_DELIMITED_JSON',
            'destinationUris': destination_uris}
        return {'extract': extract_config}


    def run_extract_job(self, job_runner, gcs_reader, source_project_id,
                        source_dataset_id, source_table_id):
        '''Runs a BigQuery extract job and reads the results.'''
        timestamp = int(time.time())
        gcs_object = 'output/%s.%s_%d.json' % (source_dataset_id, source_table_id, timestamp)
        destination_uri = gcs_reader.make_uri(gcs_object)
        job_config = make_extract_config(source_project_id, source_dataset_id, source_table_id, [destination_uri])
        if not job_runner.start_job(job_config):
            return
        print json.dumps(job_runner.get_job(), indent=2)
        job_runner.wait_for_complete()
        gcs_reader.read(gcs_object)


class PartitionReader(threading.Thread):
    '''Reads output files from a partitioned BigQuery extract job.'''

    def __init__(self, job_runner, gcs_reader, partition_id):
        threading.Thread.__init__(self)
        self.job_runner = job_runner
        self.partition_id = partition_id
        self.gcs_reader = gcs_reader
        self.gcs_object_glob = None

    def resolve_shard_path(self, path, index):
        '''Turns a glob path and an index into the expected filename.'''
        path_fmt = path.replace('*', '%012d')
        return path_fmt % (index,)

    def read_shard(self, shard):
        '''Reads the file if the file is present or returns None.'''
        resolved_object = self.resolve_shard_path(self.gcs_object_glob,
                                                  shard)
        return self.gcs_reader.read(resolved_object)

    def start(self, gcs_object_glob):
        ''' Starts the thread, reading a GCS object pattern.'''
        self.gcs_object_glob = gcs_object_glob
        threading.Thread.start(self)

    def wait_for_complete(self):
        ''' Waits for the thread to complete.'''
        self.join()

    def run(self):
        '''Waits for files to be written and reads them when they arrive.'''

        if not self.gcs_object_glob:
            raise Exception(
                'Must set the gcs_object_glob before running thread')

        print "[%d] STARTING on %s" % (self.partition_id,
                                       self.gcs_reader.make_uri(self.gcs_object_glob))
        job_done = False
        shard_index = 0
        while True:
            file_size = self.read_shard(shard_index)
            if file_size is not None:
                # Found a new file, save it, and start looking for the next one.
                shard_index += 1
            elif job_done:
                break
            else:
                # Check whether the job is done. If the job is done, we don't
                # want to exit immediately; we want to check one more time
                # for files.
                job_done = self.job_runner.get_job_state() == 'DONE'
                if not job_done:
                    # Didn't find a new path, and the job is still running,
                    # so wait a few seconds and try again.
                    time.sleep(5)
        print "[%d] DONE. Read %d files" % (self.partition_id, shard_index)


def make_extract_config(source_project_id, source_dataset_id,
                        source_table_id, destination_uris):
    '''Creates a dict containing an export job configuration.'''
    source_table_ref = {
        'projectId': source_project_id,
        'datasetId': source_dataset_id,
        'tableId': source_table_id}
    extract_config = {
        'sourceTable': source_table_ref,
        'destinationFormat': 'NEWLINE_DELIMITED_JSON',
        'destinationUris': destination_uris}
    return {'extract': extract_config}


def run_partitioned_extract_job(job_runner, gcs_readers,
                                source_project_id, source_dataset_id, source_table_id):
    '''Runs a BigQuery extract job and reads the results.'''
    destination_uris = []
    gcs_objects = []
    timestamp = int(time.time())
    partition_readers = []
    for index in range(len(gcs_readers)):
        gcs_object = 'output/%s.%s_%d.%d.*.json' % (source_dataset_id, source_table_id, timestamp, index)
        gcs_objects.append(gcs_object)
        destination_uris.append(gcs_readers[index].make_uri(gcs_object))

        # Create the reader thread for this partition.
        partition_readers.append(PartitionReader(job_runner=job_runner, gcs_reader=gcs_readers[index],
                                                 partition_id=index))

    job_config = make_extract_config(source_project_id, source_dataset_id,
                                     source_table_id, destination_uris)
    if not job_runner.start_job(job_config):
        return

    # First start all of the reader threads.
    for index in range(len(partition_readers)):
        partition_readers[index].start(gcs_objects[index])
    # Wait for all of the reader threads to complete.
    for index in range(len(partition_readers)):
        partition_readers[index].wait_for_complete()


def main(argv):
    logging.basicConfig()
    parser = ArgumentParser(description='Read BigQuery table into a text file')
    parser.add_argument('-a', '--service_account', required=True, help='Big Query service account name')
    parser.add_argument('-s', '--client_secret', required=True,
                        help='Path to client_secrets.json file required for API login')
    parser.add_argument('-c', '--credentials',
                        help=('Path to credentials file (e.g. bigquery_credentials.dat) required for API login. ',
                             'If the file is not present, the browser window will be shown and you will be asked to authenticate'))
    parser.add_argument('-k', '--keyfile', help='Path to the key file (e.g., key.p12)')
    parser.add_argument('-o', '--download_dir', default='.', help='The directory where the output will be exported')
    parser.add_argument('-p', '--project_id', required=True, help='BigQuery source project ID')
    parser.add_argument('-d', '--dataset_id', required=True, help='BigQuery source dataset ID')
    parser.add_argument('-t', '--table_id', help='Source table ID')
    parser.add_argument('-b', '--gcs_bucket', help='Google Cloud Service destination bucket')
    parser.add_argument('-n', '--partition_count', help='Partition count for partitioned reader', type=int)
    parser.add_argument('--partitioned', dest="partitioned", help='Use partitioned reader',
                        required=False, action='store_true')
    parser.set_defaults(partitioned=False)
    args = parser.parse_args()

    job_runner = JobRunner(project_id=args.project_id)
    auth = BigQuery_Auth(service_acc=args.service_account, client_secrets=args.client_secret,
                         credentials=args.credentials, key_file=args.keyfile)
    if args.partitioned:
        gcs_readers = []
        for index in range(int(args.partition_count)):
            # Note: a separate GCS reader is required per partition.
            gcs_readers.append(GcsReader(auth=auth, gcs_bucket=args.gcs_bucket,
                                   download_dir=args.download_dir))
        run_partitioned_extract_job(job_runner, gcs_readers, source_project_id=args.project_id,
                                    source_dataset_id=args.dataset_id, source_table_id=args.table_id)
    else:
        reader = SimpleReader
        gcs_reader = GcsReader(auth=auth, gcs_bucket=args.gcs_bucket, download_dir=args.download_dir)
        reader.run_extract_job(job_runner, gcs_reader, source_project_id=args.project_id,
                                    source_dataset_id=args.dataset_id, source_table_id=args.table_id)


if __name__ == "__main__":
    main(sys.argv[1:])
