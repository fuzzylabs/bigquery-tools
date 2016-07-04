from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='bigquery_utils',
    version='0.1',

    description='A set of utilities for querying and reading data from Google BigQuery',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/fuzzylabs/bigquery_utils',

    # Author details
    author='Paulius Danenas',
    author_email='danpaulius@gmail.com',
    license='MIT',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    keywords='bigquery query read utilities',
    packages=find_packages(exclude=['build', 'docs', 'tests']),
    install_requires=['google-api-python-client'],
    # Use if you want to build command-line tools as well
    # entry_points={
    #     'console_scripts': [
    #         'gcs_extract_read=gcs_extract_read:main',
    #         'gcs_reader=gcs_reader:main',
    #         'bq_metadata_reader=metadata_reader:main',
    #         'bq_table_reader=table_reader:main',
    #     ],
    # },
)