import argparse
import os
import shutil
import sys
import datetime
import re
import pandas as pd
import time
from sql_connect import sql_connect as sql
from constants import constants
from constants import paths


## paths

def build_paths(roots, sub_paths, path_sep):

    #ToDo: ¿método de paths manager?

    paths_list = [path_sep.join((root, folder)) for root in roots for folder in sub_paths]

    return paths_list


def get_match(string, expression, unknow_value = "desconocido"):

    matcher = expression.search(string)

    if matcher:
        match = matcher.group(0)
    else:
        match = unknow_value

    return match


def get_fastq_data(fastq_path, barcode_pattern, run_pattern, delete_ext = True):

    file_date = time.strftime(constants.date_formats.sql_date_format, time.gmtime(os.path.getmtime(fastq_path)))

    #Get filename

    filename = os.path.basename(fastq_path)

    if delete_ext:
        filename = os.path.splitext(filename)[0]

    #compile patterns

    barcode_re = re.compile(barcode_pattern)
    run_re = re.compile(run_pattern)

    #search

    barcode, run = [get_match(filename, expr) for expr in [barcode_re, run_re]]

    return (file_date, barcode, run)

input_paths, datalake_paths = [
    build_paths(root, paths.paths.tech_folders, paths.paths.sep) for root in [paths.paths.reads_input_roots, paths.paths.datalake_rawreads_root]
    ]

def get_folder(path):

    return os.path.basename(os.path.normpath(path))

#Crear ordenes de entrada

input_files = {
    get_folder(path) : os.listdir(path) for path in input_paths if os.path.isdir(path) and os.listdir(path)
               }


## Crear tabla para SQl

data_lake_table = pd.DataFrame(columns=constants.sql_constants.table_schema[constants.sql_constants.data_lake_table][1:])

print(data_lake_table)

