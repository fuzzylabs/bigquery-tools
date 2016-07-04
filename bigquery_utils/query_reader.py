#!/usr/bin/python2.7

__author__ = 'Paulius Danenas'

from googleapiclient.errors import HttpError
import time
from table_reader import TableReadThread

READ_CHUNK_SIZE = 64 * 1024


class QueryReader:
    def __init__(self, auth, project_id):
        self.project_id = project_id
        self.bq_service = auth.build_bq_client()
        self.columns = None

    def read(self, result_handler, query, timeout=10000, num_retries=5):
        try:
            query_request = self.bq_service.jobs()
            query_data = {
                'query': query,
                'timeoutMs': timeout,
                'allowLargeResults': True,
            }
            query_job = query_request.query(projectId=self.project_id, body=query_data).execute()
            results = []
            page_token = None
            while True:
                page = query_request.getQueryResults(pageToken=page_token,
                        **query_job['jobReference']).execute(num_retries=num_retries)
                rows = page.get('rows', [])
                result_handler.handle_rows(rows)
                page_token = page.get('pageToken')
                if not page_token:
                    break
                    result_handler.finish()
        except HttpError as err:
            # If the error is a rate limit or connection error, wait and try again.
            if err.resp.status in [403, 500, 503]:
                print '%s: Retryable error %s, waiting' % (self.thread_id, err.resp.status,)
                time.sleep(5)
            else:
                raise


class QueryReadThread(TableReadThread):
    def __init__(self, query_reader, output_file_name, query,
                 thread_id='thread', output_format='csv'):
        TableReadThread.__init__(self, None, output_file_name, thread_id, output_format)
        self.query_reader = query_reader
        self.query = query

    def get_columns(self):
        return self.query_reader.columns

    def run(self):
        print 'Reading %s' % (self.thread_id,)
        self.query_reader.read(self.get_result_handler(), self.query)
