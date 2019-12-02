# -*- coding: utf-8 -*-
"""
This code demonstrates how to use dedupe with a comma separated values
(CSV) file. All operations are performed in memory, so will run very
quickly on datasets up to ~10,000 rows.

We start with a CSV file containing our messy data. In this example,
it is listings of early childhood education centers in Chicago
compiled from several different sources.

The output will be a CSV with our clustered results.

For larger datasets, see our [mysql_example](mysql_example.html)
"""
from future.builtins import next

import os
import csv
import re
import logging
import optparse
#import sys

import dedupe
from unidecode import unidecode
#sys.setdefaultencoding('utf-8')

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. This
# code block lets you change the level of loggin on the command
# line. You don't need it if you don't want that. To enable verbose
# logging, run `python examples/csv_example/csv_example.py -v`
optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING 
if opts.verbose:
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)

# ## Setup

input_file = 'all_cipwithrole.csv'
output_file = 'csv_example_output.csv'
settings_file = 'csv_example_learned_settings'
training_file = 'csv_example_training.json'

def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    try : # python 2/3 string differences
        column = column.decode('utf-8', 'ignore')
    except AttributeError:
        pass
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    # If data is missing, indicate that by setting the value to `None`
    if not column:
        column = None
    return column

def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records, 
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename,encoding='utf-8',errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            row_id = int(row['AML_PTY_ID'])
            print(row_id)
            data_d[row_id] = dict(clean_row)

    return data_d

print('importing data ...')
data_d = readData(input_file)

# If a settings file already exists, we'll just load that and skip training
if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as f:
        deduper = dedupe.StaticDedupe(f)
else:
    # ## Training

    # Define the fields dedupe will pay attention to
#RN,AML_PTY_ID,NAM,REG_NAM,FRST_NAM,LST_NAM,REG_FL_NAM,TYP_CDE,FORM_DOB,ALIAS_TYP_CDE,ALIAS_VAL_TXT,ADR_LN1_TXT,REG_ADR_LN1_TXT#,ADR_LN2_TXT,CTY_NAM,REG_CTY_NAM,ST_NAM,REG_ST_NAM,PSTL_CDE,REG_PSTL_CDE,CNTRY_PHYSICAL,CORF,ROLE,DOB_C,GOC_C,ADDP_C,ADD_C,COR#_C
    fields = [
        {'field' : 'TYP_CDE', 'type': 'Exact'},
        {'field' : 'ALIAS_VAL_TXT', 'type': 'Exact', 'has missing' : True},
        {'field' : 'FORM_DOB', 'type': 'Exact', 'has missing' : True},
        {'field' : 'REG_FL_NAM', 'type': 'String'},
        {'field' : 'ADR_LN1_TXT', 'type': 'String', 'has missing' : True},
        {'field' : 'REG_CTY_NAM', 'type': 'String', 'has missing' : True},
        {'field' : 'REG_ST_NAM', 'type': 'String', 'has missing' : True},
        {'field' : 'CNTRY_PHYSICAL', 'type': 'Exact'},
        #{'field' : 'Phone', 'type': 'String', 'has missing' : True},
        ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

