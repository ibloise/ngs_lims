"""
Created Mar 7, 2023
"""

class sql_constants:
    """
    constants of sql
    """

    #schemas
    lims_schema = "NGS_LIMS"

    #tables
    data_lake_table = "data_lake"


    #fields
    sample_col = "sample_name"

    #data_lake_table
    data_lake_id = "id"
    data_lake_filename = "file_name"
    data_lake_filedate = "file_date"
    data_lake_barcode = "barcode"
    data_lake_run = "run"
    data_lake_path = "path"
    data_lake_tech = "technology"
    data_lake_reads = "reads"
    data_lake_sample = "sample_name"

    ## table with fields

    table_schema = {
        data_lake_table : [
        data_lake_id,
        data_lake_filename,
        data_lake_filedate,
        data_lake_barcode,
        data_lake_run,
        data_lake_tech,
        data_lake_reads,
        data_lake_sample
        ]
    }

class date_formats:
    """
    constants for date formats
    """

    job_format = "%Y_%m_%d-%H-%M-%S"
    sql_date_format = "%Y-%m-%d"

class prefix:

    job_prefix = "_job_"


class re_patterns:

    iontorrent_barcode = ["IonXpress_\d{3}"]

    iontorrent_run = [("R_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}_user_GSS5-\d+-\d+.*")]


class technology:
    tech_levels = ["iontorrent", "illumina", "nanopore"]

    iontorrent_reads_levels = ["single-end"]

    illumina_reads_levels = ["forward", "reverse"]