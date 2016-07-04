#!/usr/bin/python2.7

# Adopted from code provided in https://code.google.com/archive/p/bigquery-e2e/

'''Handles credentials and authorization.

This module is used by the sample scripts to handle credentials and
generating authorized clients. The module can also be run directly
to print out the HTTP authorization header for use in curl commands.
Note that the first time this module is run (either directly or via
a sample script) it will trigger the OAuth authorization process.
'''

from argparse import ArgumentParser
import sys
import json
import os
import httplib2
from apiclient import discovery
from oauth2client.client import flow_from_clientsecrets
from oauth2client import tools
from oauth2client.file import Storage

HAS_CRYPTO = False
try:
    # Some systems may not have OpenSSL installed so can't use SignedJwtAssertionCredentials.
    from oauth2client.client import SignedJwtAssertionCredentials
    HAS_CRYPTO = True
except ImportError:
    pass

BIGQUERY_SCOPE = 'https://www.googleapis.com/auth/bigquery'

class BigQuery_Auth:
    """
    Handles authorization. Example parameters for initialization
    service_acct = ('<service account id>@developer.gserviceaccount.com')
    key_file = 'key.p12'
    client_secrets = 'client_secrets.json'
    credentials = 'bigquery_credentials.dat'
    """

    def __init__(self, service_acc, client_secrets, credentials=None, key_file=None):
        self.SERVICE_ACCT = (service_acc)
        self.CLIENT_SECRETS = client_secrets
        self.CREDENTIALS_FILE = credentials
        self.KEY_FILE = key_file

    def get_creds(self):
        '''Get credentials for use in API requests.

        Generates service account credentials if the key file is present,
        and regular user credentials if the file is not found.
        '''
        if self.KEY_FILE is not None and os.path.exists(self.KEY_FILE):
            return self.get_service_acct_creds(self.SERVICE_ACCT, self.KEY_FILE)
        else:
            return self.get_oauth2_creds()


    def get_oauth2_creds(self):
        '''Generates user credentials.
        Will prompt the user to authorize the client when run the first time.
        Saves the credentials in self.CREDENTIALS_FILE.
        '''
        flow = flow_from_clientsecrets(self.CLIENT_SECRETS, scope=BIGQUERY_SCOPE)
        storage = Storage(os.path.expanduser(self.CREDENTIALS_FILE))
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            flags = tools.argparser.parse_args([])
            credentials = tools.run_flow(flow, storage, flags)
        else:
            # Make sure we have an up-to-date copy of the credentials
            credentials.refresh(httplib2.Http())
        return credentials


    def get_service_acct_creds(self, service_acct, key_file):
        '''Generate service account credentials using the given key file.
        service_acct: service account ID.
        key_file: path to file containing private key.
        '''
        if not HAS_CRYPTO:
            raise Exception("Unable to use cryptographic functions. Try installing OpenSSL")
        with open(key_file, 'rb') as f:
            key = f.read()
        return SignedJwtAssertionCredentials(service_acct, key, BIGQUERY_SCOPE)


    def authorize(self, credentials):
        '''Construct a HTTP client that uses the supplied credentials.'''
        return credentials.authorize(httplib2.Http())

    def print_creds(self, credentials):
        '''Prints the authorization header to use in HTTP requests.'''
        cred_dict = json.loads(credentials.to_json())
        if 'access_token' in cred_dict:
            print 'Authorization: Bearer %s' % (cred_dict['access_token'],)
        else:
            print 'Credentials: %s' % (cred_dict,)

    def build_bq_client(self):
        '''Constructs a bigquery client object.'''
        return discovery.build('bigquery', 'v2', http=self.get_creds().authorize(httplib2.Http()))

    def build_gcs_client(self):
        '''Constructs a Google Cloud Storage client object.'''
        return discovery.build('storage', 'v1', http=self.get_creds().authorize(httplib2.Http()))


def main(argv):
    parser = ArgumentParser(description='Read BigQuery table into a text file')
    parser.add_argument('-a', '--service_account', required=True, help='Big Query service account name')
    parser.add_argument('-s', '--client_secret', required=True,
                        help='Path to client_secrets.json file required for API login')
    parser.add_argument('-c', '--credentials',
                        help=('Path to credentials file (e.g. bigquery_credentials.dat) required for API login. ',
                             'If the file is not present, the browser window will be shown and you will be asked to authenticate'))
    parser.add_argument('-k', '--keyfile', help='Path to the key file (e.g., key.p12)')
    args = parser.parse_args()

    auth = BigQuery_Auth(service_acc=args.service_account, client_secrets=args.client_secret,
                         credentials=args.credentials, key_file=args.keyfile)
    auth.print_creds(auth.get_creds())


if __name__ == "__main__":
    main(sys.argv[1:])