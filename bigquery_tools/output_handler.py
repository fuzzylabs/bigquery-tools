__author__ = 'Paulius Danenas'

import os
import json
import csv

class ResultHandler:
    '''Abstract class to handle reading TableData rows.'''

    def handle_rows(self, rows):
        '''Process one page of results.'''
        pass


class ColumnarResultHandler(ResultHandler):

    def __init__(self):
        self.columns = None

    def set_columns(self, columns):
        self.columns = list(columns)


class FileResultHandler(ResultHandler):
    '''Result handler that saves rows to a file.'''

    def __init__(self, output_file_name):
        self.output_file_name = output_file_name
        self.output_file = None
        print 'Writing results to %s' % (output_file_name,)

    def __enter__(self):
        self.make_output_dir()
        self.output_file = open(self.output_file_name, 'wb')
        return self

    def finish(self, type=None, value=None, traceback=None):
        if self.output_file:
            self.output_file.close()
            self.output_file = None
        print 'Finished writing results'

    def make_output_dir(self):
        '''Creates an output directory for the downloaded results.'''
        output_dir = os.path.dirname(self.output_file_name)
        if len(output_dir) == 0 or (os.path.exists(output_dir) and os.path.isdir(output_dir)):
            return
        os.makedirs(output_dir)

    def handle_rows(self, rows):
        if self.output_file is None:
            self.__enter__()
        self.output_file.write(''.join(rows))


class JSONResultHandler(FileResultHandler):

    def __init__(self, output_file_name):
        FileResultHandler.__init__(self, output_file_name)
        self.output = list()

    def finish(self, type=None, value=None, traceback=None):
        if self.output_file is None:
            self.__enter__()
        self.output_file.write(json.dumps(self.output))
        FileResultHandler.finish(self, type, value, traceback)

    def handle_rows(self, rows):
        self.output.extend(rows)


class CSVResultHandler(FileResultHandler, ColumnarResultHandler):

    def __init__(self, output_file_name, columns=None, sep=';'):
        FileResultHandler.__init__(self, output_file_name)
        self.csv_file = None
        self.columns = columns
        self.sep = sep

    def __enter__(self):
        FileResultHandler.__enter__(self)
        self.csv_file = csv.writer(self.output_file, delimiter=self.sep,
                                   quoting=csv.QUOTE_MINIMAL)
        if self.columns:
            self.csv_file.writerow(self.columns)
        return self

    def handle_rows(self, rows):
        if self.output_file is None:
            self.__enter__()
        for row in rows:
            self.csv_file.writerow([field['v'].encode("ascii", "ignore")
                                    if field['v'] is not None else None for field in row['f']])
