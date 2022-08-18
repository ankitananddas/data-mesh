"""
This script contains util functions for saving output data
"""

import exceptions


def store_into_redshift(spark, df, table_name, mode):
    """
    This is utils function for storing data into redshift
    :param spark: sparkSession
    :param df: dataframe object to be exported to redshift
    :param table_name: destination table name
    :param mode: append/overwrite
    :return: 0 if successful otherwise respective exception
    """
    db_config = spark['redshift_config']
    host_name = db_config['REDSHIFT_HOSTNAME']
    port = db_config['REDSHIFT_PORT']
    user = db_config['REDSHIFT_USERNAME']
    password = db_config['REDSHIFT_PASSWORD']
    database = db_config['REDSHIFT_LANDING_DATABASE']
    driver = "com.amazon.redshift.jdbc42.Driver"
    mode = mode.lower()
    if mode not in ['overwrite', 'append']:
        raise exceptions.UnknownSparkWriteModeException

    try:
        df.write.format("jdbc")\
            .option("url", f"jdbc:redshift://{host_name}:{port}/{database}")\
            .option("driver", driver)\
            .option("dbtable", table_name)\
            .option("user", user)\
            .option("password", password)\
            .mode(mode).save()
    except:
        raise exceptions.RedshiftWriteException

    return 0
