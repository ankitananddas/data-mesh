"""
This script contains util functions for reading input data
"""

import exceptions


def get_collection_col_list(spark, collection_name, redshift_creds):
    """
    This is the util function to get the to get column list of collections
    :param spark: spark session
    :param collection_name: collection name to get the to get column list
    :param redshift_creds: dict of redshift creds
    :return: list of columns for collection
    """
    host_name = redshift_creds['REDSHIFT_HOSTNAME']
    port = redshift_creds['REDSHIFT_PORT']
    user = redshift_creds['REDSHIFT_USERNAME']
    password = redshift_creds['REDSHIFT_PASSWORD']
    database = redshift_creds['REDSHIFT_LANDING_DATABASE']
    query = f"select * from outreach.table_column_list where table_name = '{collection_name}'"
    try:
        col_list = spark.read.format("jdbc") \
            .option("url", f"jdbc:redshift://{host_name}:{port}/{database}") \
            .option("driver", "com.amazon.redshift.jdbc42.Driver")\
            .option('query', query) \
            .option("user", user)\
            .option("password", password).load()
    except:
        raise exceptions.RedshiftReadException

    # TODO: check performance implications of collect/distinct over here
    col_list = col_list.select("Column_Name").rdd.flatMap(lambda x: x).collect()
    return col_list


def get_last_run_datetime_from_redshift(spark, redshift_creds, table_name=""):
    """
    This is the util function to get the last run time for a table in order to perform incremental load
    :param spark: spark session
    :param redshift_creds: dict of redshift creds
    :param table_name: table for which we need to check the last timestamp
    :return: last run timestamp in unix timestamp format
    """
    host_name = redshift_creds['REDSHIFT_HOSTNAME']
    port = redshift_creds['REDSHIFT_PORT']
    user = redshift_creds['REDSHIFT_USERNAME']
    password = redshift_creds['REDSHIFT_PASSWORD']
    database = redshift_creds['REDSHIFT_LANDING_DATABASE']

    query = "select max(LAST_PROCESSED_DATETIME) as LAST_PROCESSED_DATETIME from outreach.LAST_PROCESSED_DATE"
    try:
        read_last_run_dt = spark.read.format("jdbc") \
            .option("url", f"jdbc:redshift://{host_name}:{port}/{database}") \
            .option("driver", "com.amazon.redshift.jdbc42.Driver")\
            .option('query', query) \
            .option("user", user)\
            .option("password", password).load()
    except:
        raise exceptions.RedshiftReadException

    # Loading date value into a variable after converting it into epoch in milliseconds
    df = read_last_run_dt.withColumn('LAST_PROCESSED_DATETIME',
                                     read_last_run_dt['LAST_PROCESSED_DATETIME'].cast('long') * 1000)
    # TODO: check performance implications of collect/distinct over here
    last_processed_dt = df.select('LAST_PROCESSED_DATETIME').distinct().collect()
    last_run_time = last_processed_dt[0][0]
    return last_run_time


def read_from_mongo(spark, database_name, collection_name, pipeline="", col_fields=None):
    """
    This is the function to read data from mongo
    :param spark: spark session
    :param database_name: mongo database name
    :param pipeline: mongo aggregation pipelines
    :param col_fields: columns to read
    :param collection_name: collection to read from database
    :return: mongodb collection as dataframe is successful otherwise respective error
    """
    try:
        df = spark.read.format("mongodb") \
            .option("database", database_name) \
            .option("collection", collection_name) \
            .option("aggregation.pipeline", pipeline) \
            .load()
    except:
        raise exceptions.MongoReadException

    if col_fields:
        df = df.select(col_fields)

    return df
