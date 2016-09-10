#!/usr/bin/python2.7

__author__ = 'Paulius Danenas'

import time
from googleapiclient.errors import HttpError
from output_handler import ColumnarResultHandler
from table_reader import TableReadThread
from progressbar import Counter, ProgressBar, Timer

READ_CHUNK_SIZE = 64 * 1024


class QueryReader:
    def __init__(self, auth, project_id):
        self.project_id = project_id
        self.bq_service = auth.build_bq_client()
        self.columns = None

    def read(self, result_handler, query, timeout=10000, num_retries=5, inlineUDF=None, udfURI=None):
        """
        Retrieves query results
        :param result_handler: ResultHandler which is used to handle results
        :param query: BigQuery query which is executed
        :param timeout: Maximum timeout
        :param num_retries: Maximum numer of retries
        :param inlineUDF: UDF code in Javascript, which is associated with particular BQ query/table.
        Note: if this is set, it will be preferred over udfURI property
        :param udfURI: An array of URIs which point to the relevant UDF resources (e.g., Javascript files in Google Cloud)
        E.g.: [gs://bucket/my-udf.js]
        Note: if this is NOT set, inlineUDF property will be used (if set)
        """
        try:
            query_request = self.bq_service.jobs()
            udfResource = []
            if inlineUDF:
                udfResource.append({'inlineCode': inlineUDF})
            else:
                if udfURI:
                    udfResource.append({'resourceUri': udfURI})

            query_data = {
                'query': query,
                'timeoutMs': timeout,
                'allowLargeResults': True,
                'userDefinedFunctionResources': udfResource
            }
            query_job = query_request.query(projectId=self.project_id, body=query_data).execute()
            self.columns = [field['name'] for field in query_job['schema']['fields']]
            if isinstance(result_handler, ColumnarResultHandler):
                result_handler.set_columns(self.columns)
            page_token = None
            widgets = ['Retrieved rows: ', Counter(), ' (', Timer(), ')']
            pbar = ProgressBar(widgets=widgets)
            pbar.start()
            pbar.maxval = 0
            i = 0
            while True:
                page = query_request.getQueryResults(pageToken=page_token,
                                                     **query_job['jobReference']).execute(num_retries=num_retries)
                rows = page.get('rows', [])
                if rows:
                    i += len(rows)
                    pbar.maxval = i
                    pbar.update(i)
                result_handler.handle_rows(rows)
                page_token = page.get('pageToken')
                if not page_token:
                    result_handler.finish()
                    break
            pbar.finish()
        except HttpError as err:
            # If the error is a rate limit or connection error, wait and try again.
            if err.resp.status in [403, 500, 503]:
                print '%s: Retryable error %s, waiting' % (self.thread_id, err.resp.status,)
                time.sleep(5)
            else:
                raise


class QueryReadThread(TableReadThread):
    def __init__(self, query_reader, output_file_name, query,
                 thread_id='thread', output_format='csv', sep=';'):
        TableReadThread.__init__(self, None, output_file_name, thread_id, output_format, sep)
        self.query_reader = query_reader
        self.query = query

    def get_columns(self):
        return None

    def run(self):
        print 'Reading %s' % (self.thread_id,)
        self.query_reader.read(self.get_result_handler(), self.query)
