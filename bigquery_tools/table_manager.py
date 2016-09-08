import json
from apiclient.errors import HttpError

class GenericGBQException(Exception):
    """
    Raised when an unrecognized Google API Error occurs.
    """
    pass

class TableManager:

    def __init__(self, auth):
        self.service = auth.build_bq_client()

    def create_table(self, dataset_id, table_name, schema, project_id=None):
        dataset_ref = {'datasetId': dataset_id,
                       'projectId': project_id}
        table_ref = {'tableId': table_name,
                     'datasetId': dataset_id,
                     'projectId': project_id}
        table = {'tableReference': table_ref,
                 'schema': schema
                 }
        table = self.service.tables().insert(
            body=table, **dataset_ref).execute()
        return table

    def drop_table(self, dataset_id, table, project_id=None):
        dataset_ref = {'datasetId': dataset_id,
                       'projectId': project_id,
                       'tableId': table}
        table = self.service.tables().delete(
            **dataset_ref).execute()
        return table

    def dataset_exists(self, project_id, dataset_id):
        """ Check if a dataset exists in Google BigQuery
        """
        try:
            self.service.datasets().get(
                projectId=project_id,
                datasetId=dataset_id).execute()
            return True
        except HttpError as ex:
            if ex.resp.status == 404:
                return False
            else:
                self.process_http_error(ex)

    def table_exists(self, project_id, dataset_id, table_id):
        """ Check if a table exists in Google BigQuery
        """
        try:
            self.service.tables().get(
                projectId=project_id,
                datasetId=dataset_id,
                tableId=table_id).execute()
            return True
        except HttpError as ex:
            if ex.resp.status == 404:
                return False
            else:
                self.process_http_error(ex)

    @staticmethod
    def process_http_error(ex):
        # See `BigQuery Troubleshooting Errors
        # <https://cloud.google.com/bigquery/troubleshooting-errors>`__
        status = json.loads(ex.content)['error']
        errors = status.get('errors', None)
        if errors:
            for error in errors:
                reason = error['reason']
                message = error['message']
                raise GenericGBQException(
                    "Reason: {0}, Message: {1}".format(reason, message))
        raise GenericGBQException(errors)