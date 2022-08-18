"""
This script contains utils functions for flattening of dataframe
"""


import json

from pyspark.sql.functions import col, from_json, explode_outer, expr
from pyspark.sql.types import StringType, ArrayType, StructType

import exceptions


def flatten_struct(df, col_name, col_schema):
    """
    Flatten the schema of struct
    :param df: dataframe object
    :param col_name: column of struct type
    :param col_schema: internal schema of struct
    :return: flattened struct
    """
    expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in [n.name for n in col_schema]]
    df = df.select("*", *expanded).drop(col_name)
    return df


def flatten_array(df, col_name, col_list=None):
    """
    Flatten/explode the array column-wise or row-wise
    :param df: dataframe to flatten
    :param col_name: column of array type
    :param col_list: list of columns to be exploded column wise
    :return: flattened array
    """
    remove_column = False
    if col_list is None:
        col_list = []
    if col_name in col_list:
        # exploding column wise directly if is not an array of struct
        remove_column = True
        if not isinstance(df.schema[col_name].dataType.elementType, StructType):
            df = df.select("*", *[(col(f"{col_name}")[x]).alias(f'{col_name}_other_{x + 1}') for x in range(0, 4)])
            df = df.drop(f'{col_name}')
        else:
            # parse invalid key if it is present otherwise send respective exception
            try:
                df = df.withColumn(f'valid_{col_name}', expr(f"filter({col_name}, x -> x.invalid = False)"))
            except:
                raise exceptions.FlatteningArrayOfStructIsInvalidKeyMissing

            # if invalid key is present parse isPrimary key
            try:
                df = df. \
                    withColumn(f"{col_name}_primary", expr(f"filter(valid_{col_name}, x -> x.isPrimary = True)")). \
                    withColumn(f"{col_name}_other", expr(f"filter(valid_{col_name}, x -> x.isPrimary = False)")). \
                    drop(col_name, f'valid_{col_name}')
                df = df.select("*",
                               *[(col(f"{col_name}_other")[x]).alias(f'{col_name}_other_{x + 1}') for x in range(0, 4)])
                df = df.drop(f'{col_name}_other')
            except:
                raise exceptions.FlatteningArrayOfStructIsPrimaryKeyMissing
    else:
        # exploding row wise
        df = df.withColumn(col_name, explode_outer(col_name))
    return df, remove_column


def flatten_str(spark, df, col_name):
    """
    Flatten the struct like string to a proper pyspark struct
    :param spark: spark session
    :param df: dataframe to flatten
    :param col_name: column name of string type column
    :return: struct like string converted to string
    """
    # Trying to load any non-null value of string column as dictionary
    # If json.loads fails then the column is actually a string not a struct
    # else it is a struct then we need to parse it using from_json method
    remove_column = True
    try:
        json.loads(eval(f'df.filter(df.{col_name}.isNotNull()).head().{col_name}')).keys()
    except:
        return df, remove_column
    remove_column = False
    json_schema = spark.read.ujson(df.rdd.map(lambda row: eval(f'row.{col_name}'))).schema
    df = df.withColumn(f'{col_name}_struct', from_json(col(col_name), json_schema))
    df = df.drop(col_name)
    df = df.withColumnRenamed(f'{col_name}_struct', col_name)
    return df, remove_column


def prepare_complex_fields(df, columns_to_remove=None):
    """
    utils function for preparing a dictionary of complex fields i.e. of datatype - Struct, String, Array
    :param df: dataframe object
    :param columns_to_remove: list of columns to be removed from complex fields
    :return: dict of complex fields
    """
    if columns_to_remove is None:
        columns_to_remove = []
    return dict([(field.name, field.dataType) for field in df.schema.fields
                 if (field.name not in columns_to_remove) and (isinstance(field.dataType, ArrayType)
                                                               or isinstance(field.dataType, StructType)
                                                               or isinstance(field.dataType, StringType))])


def flatten(df, col_list=None):
    """
    Driver function for flattening a data frame containing columns of various datatypes
    :param df: dataframe object
    :param col_list: list of columns to be exploded column wise
    :return: flattened dataframe
    """
    complex_fields = prepare_complex_fields(df)
    columns_to_remove = []
    if col_list is None:
        col_list = []
    while len(complex_fields) != 0:
        remove_column = False
        col_name = list(complex_fields.keys())[0]
        if isinstance(complex_fields[col_name], StructType):
            df = flatten_struct(df, col_name, complex_fields[col_name])
        elif isinstance(complex_fields[col_name], ArrayType):
            try:
                df, remove_column = flatten_array(df, col_name, col_list)
            except:
                raise  # forward the exception raised in previous step
        else:
            df, remove_column = flatten_str(df, col_name)
        if remove_column:
            columns_to_remove.append(col_name)
        complex_fields = prepare_complex_fields(df, columns_to_remove)
    return df
