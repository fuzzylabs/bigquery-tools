#!/usr/bin/python2.7

# Adopted from code provided in https://code.google.com/archive/p/bigquery-e2e/

'''Helper class to download GCS files.

This module contains code to read files from Google Cloud Storage.

will read the GCS path gs://gcs_bucket/gcs_object and download
it to the directory download_dir. If download_dir is not specified,
the file will just be checked for existence and not actually downloaded.
auth is the BigQuery_Auth object used to connect BQ

Usage from the command line:
python gcs_reader.py [options]
'''

import os
import sys
from argparse import ArgumentParser
# Imports from the Google API client:
from apiclient.errors import HttpError
from apiclient.http import MediaIoBaseDownload
from auth import BigQuery_Auth

# Number of bytes to download per request.
CHUNKSIZE = 1024 * 1024


class GcsReader:
    '''Reads files from Google Cloud Storage.
    Verifies the presence of files in Google Cloud Storage. Will download
    the files as well if download_dir is not None.
    '''

    def __init__(self, auth, gcs_bucket, download_dir=None):
        self.gcs_service = auth.build_gcs_client()
        self.auth = auth
        self.gcs_bucket = gcs_bucket
        self.download_dir = download_dir

    def make_uri(self, gcs_object):
        '''Turn a bucket and object into a Google Cloud Storage path.'''
        return 'gs://%s/%s' % (self.gcs_bucket, gcs_object)

    def check_gcs_file(self, gcs_object):
        '''Returns a tuple of (GCS URI, size) if the file is present.'''
        try:
            metadata = self.gcs_service.objects().get(
                bucket=self.gcs_bucket, object=gcs_object).execute()
            uri = self.make_uri(gcs_object)
            return (uri, int(metadata.get('size', 0)))
        except HttpError as err:
            # If the error is anything except a 'Not Found' print the error.
            if err.resp.status <> 404:
                print err
            return (None, None)

    def make_output_dir(self, output_file):
        '''Creates an output directory for the downloaded results.'''
        output_dir = os.path.dirname(output_file)
        if os.path.exists(output_dir) and os.path.isdir(output_dir):
            # Nothing to do.
            return
        os.makedirs(output_dir)

    def complete_download(self, media):
        while True:
            # Download the next chunk, allowing 3 retries.
            _, done = media.next_chunk(num_retries=3)
            if done: return

    def download_file(self, gcs_object):
        '''Downloads a GCS object to directory download_dir.'''
        output_file_name = os.path.join(self.download_dir, gcs_object)
        self.make_output_dir(output_file_name)
        with open(output_file_name, 'w') as out_file:
            request = self.gcs_service.objects().get_media(
                bucket=self.gcs_bucket, object=gcs_object)
            media = MediaIoBaseDownload(out_file, request, chunksize=CHUNKSIZE)

            print 'Downloading:\n%s to\n%s' % (
                self.make_uri(gcs_object), output_file_name)
            self.complete_download(media)

    def read(self, gcs_object):
        '''Read the file and returns the file size or None if not found.'''
        uri, file_size = self.check_gcs_file(gcs_object)
        if uri is None:
            return None
        print '%s size: %d' % (uri, file_size)
        if self.download_dir is not None:
            self.download_file(gcs_object)
        return file_size

    def list_bucket(self):
        """Returns a list of metadata of the objects within the given bucket."""

        # Create a request to objects.list to retrieve a list of objects.
        fields_to_return = 'nextPageToken,items(name,size,contentType,metadata(my-key))'
        req = self.gcs_service.objects().list(bucket=self.gcs_bucket, fields=fields_to_return)
        all_objects = []
        # If you have too many items to list in one request, list_next() will
        # automatically handle paging with the pageToken.
        while req:
            resp = req.execute()
            all_objects.extend(resp.get('items', []))
            req = self.gcs_service.objects().list_next(req, resp)
        return all_objects


def main(argv):
    parser = ArgumentParser(description='Read BigQuery table into a text file')
    parser.add_argument('-a', '--service_account', required=True, help='Big Query service account name')
    parser.add_argument('-s', '--client_secret', required=True,
                        help='Path to client_secrets.json file required for API login')
    parser.add_argument('-c', '--credentials',
                        help=('Path to credentials file (e.g. bigquery_credentials.dat) required for API login. ',
                             'If the file is not present, the browser window will be shown and you will be asked to authenticate'))
    parser.add_argument('-k', '--keyfile', help='Path to the key file (e.g., key.p12)')
    parser.add_argument('-o', '--download_dir', default='.', help='The directory where the output will be exported')
    parser.add_argument('-b', '--gcs_bucket', help='Google Cloud Service bucket where the object is put')
    parser.add_argument('-f', '--gcs_object', help='The object to be downloaded')
    args = parser.parse_args()

    gcs_reader = GcsReader(auth=BigQuery_Auth(service_acc=args.service_account, client_secrets=args.client_secret,
                                              credentials=args.credentials, key_file=args.keyfile),
                           gcs_bucket=args.gcs_bucket, download_dir=args.download_dir)
    gcs_reader.read(args.gcs_object)

if __name__ == "__main__":
    main(sys.argv[1:])
