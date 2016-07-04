#!/usr/bin/python2.7
#
'''
Retrieve main metadata and statistics of table, dataset, etc.
Usage from the command line:
python metadata_reader.py [options]
'''

__author__ = 'Paulius Danenas'

from googleapiclient.errors import HttpError   # pip install google-api-python-client
from pprint import pprint
from argparse import ArgumentParser
import io
import csv
import sys
from auth import BigQuery_Auth

class MetadataReader:

    def __init__(self, auth):
        self.auth = auth
        self.service = auth.build_bq_client()

    def list_tables(self, project_id, dataset_id):
        try:
            tables = self.service.tables()
            tlist = tables.list(projectId=project_id, datasetId=dataset_id).execute()
            return [field['id'] for field in tlist['tables']]
        except HttpError as err:
            print 'Error in listTables:', pprint(err.content)


    def table_columns(self, project_id, dataset_id, table_id):
        try:
            tableCollection = self.service.tables()
            tableReply = tableCollection.get(projectId=project_id,
                                             datasetId=dataset_id,
                                             tableId=table_id).execute()
            return {field['name']: field['type'] for field in tableReply['schema']['fields']}
        except HttpError as err:
            print 'Error in query table data: ', pprint(err)


    def table_stats(self, project_id, dataset_id, table_id):
        """
        Get basic statistics for each column, such as number of counts null, non-null and distinct vales
        """
        columns = self.table_columns(project_id, dataset_id, table_id)
        # Create query for counting null and non null values for each column
        stats_query = 'select '
        for col in columns.keys():
            stats_query += ", ".join(["'{0}', sum(case when {0} is null then 1 else 0 end)",
                                      "sum(case when {0} is not null then 1 else 0 end)",
                                      "count(distinct {0})"
                                      ]).format(col, dataset_id, table_id) + ", "
        stats_query = stats_query[:-2]
        stats_query += ' FROM [%s.%s]' % (dataset_id, table_id)

        try:
            query_request = self.service.jobs()
            query_data = {
                'query': (stats_query)
            }
            query_response = query_request.query(projectId=project_id, body=query_data).execute()
            values = []
            for row in query_response['rows']:
                values = [field['v'] for field in row['f']]
            colnames = [values[i] for i in range(len(values)) if i % 4 == 0]
            null_counts = [values[i] for i in range(len(values)) if i % 4 == 1]
            nonnull_counts = [values[i] for i in range(len(values)) if i % 4 == 2]
            distinct_counts = [values[i] for i in range(len(values)) if i % 4 == 3]

            output =  io.BytesIO()
            writer = csv.writer(output)
            writer.writerow(['Column name', 'Empty', 'Non-empty', 'Unique'])
            for i in xrange(0, len(colnames)):
                writer.writerow([colnames[i], null_counts[i], nonnull_counts[i], distinct_counts[i]])
            return output.getvalue()

        except HttpError as err:
            print('Error: {}'.format(err.content))
            raise err


def main(argv):
    parser = ArgumentParser(description='Read BigQuery table into a text file')
    parser.add_argument('-a', '--service_account', required=True, help='Big Query service account name')
    parser.add_argument('-s', '--client_secret', required=True,
                        help='Path to client_secrets.json file required for API login')
    parser.add_argument('-c', '--credentials',
                        help=('Path to credentials file (e.g. bigquery_credentials.dat) required for API login. ',
                             'If the file is not present, the browser window will be shown and you will be asked to authenticate'))
    parser.add_argument('-k', '--keyfile', help='Path to the key file (e.g., key.p12)')
    parser.add_argument('-p', '--project_id', required=True, help='BigQuery project ID')
    parser.add_argument('-d', '--dataset_id', required=True, help='BigQuery dataset ID')
    parser.add_argument('-t', '--table_id', help='Table ID')
    parser.add_argument('-l', '--list_tables', dest="list_tables", help='List tables in the given dataset', action='store_true')
    parser.add_argument('--table_cols', dest="table_cols", help='Column metadata for table table_id', action='store_true')
    parser.add_argument('--table_stats', dest="table_stats", help='Core statistics for the table table_id', action='store_true')
    parser.set_defaults(list_tables=False, table_cols=False, table_stats=False)
    args = parser.parse_args()

    reader = MetadataReader(auth=BigQuery_Auth(service_acc=args.service_account, client_secrets=args.client_secret,
                                              credentials=args.credentials, key_file=args.keyfile))
    if (args.list_tables):
        print 'Tables in dataset %s:' % (args.dataset_id)
        pprint(reader.list_tables(args.project_id, args.dataset_id))
        print
    if (args.table_id):
        if (args.table_cols):
            print 'Columns in table %s' % (args.table_id)
            pprint(reader.table_columns(args.project_id, args.dataset_id, args.table_id))
            print
        if (args.table_stats):
            print 'Statistics in table %s' % (args.table_id)
            print(reader.table_stats(args.project_id, args.dataset_id, args.table_id))
            print


if __name__ == "__main__":
    main(sys.argv[1:])