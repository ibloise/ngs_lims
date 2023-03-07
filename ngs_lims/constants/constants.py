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



class date_formats:
    """
    constants for date formats
    """

    job_format = "%Y_%m_%d-%H-%M-%S"

class prefix:

    job_prefix = "_job_"