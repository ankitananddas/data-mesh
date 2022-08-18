"""
Script for landing layer of campaigns collection from mongo to redshift
"""

from ..utils import read_data, flatten, exceptions, generate_traceback


def load_campaign_data_into_landing_layer(spark, mongo_creds, redshift_creds, logger):
    """
    Function for loading campaign data into landing layer
    :param spark: spark session
    :param mongo_creds: dict with mongo creds
    :param redshift_creds: dict with redshift creds
    :param logger: logger obj
    :return: dataframe to be written in redshift
    """
    # TODO: Add snippets for catching exceptions
    # TODO: Add loggers
    collection_name = 'campaign'
    mongo_database = mongo_creds['MONGO_DATABASE']
    last_run_time = read_data.get_last_run_datetime_from_redshift(spark, redshift_creds)  # 1441097843000
    col_fields = read_data.get_collection_col_list(spark, collection_name, redshift_creds)
    pipeline = "{'$match': {$expr: {$gte : [{$toDate: \"$param1\"}, {$toDate: param2}]}}}". \
        replace("param2", str(last_run_time)).replace("param1", "updatedAt")
    df = read_data.read_from_mongo(spark, mongo_database, collection_name, pipeline, col_fields)
    df = flatten.flatten(df)
    return df
