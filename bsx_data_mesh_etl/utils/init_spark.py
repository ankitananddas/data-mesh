"""
This script contains the utils function for initializing spark session and getting DB creds
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import boto3

from ..constants.common import ConfigConst
import exceptions


def get_mongo_connection(ssm_client, env_name):
    """
    Function to get mongo connection details from AWS parameter store
    :param ssm_client: boto3 client to interact with aws parameter store
    :param env_name: stage/prod
    :return: dict with mongo creds
    """
    secrets = {
        'MONGO_HOSTNAME': ssm_client.get_parameter(Name=f'/{env_name}/mongo/MONGO_HOSTNAME')['Parameter']['Value'],
        'MONGO_DATABASE': ssm_client.get_parameter(Name=f'/{env_name}/mongo/MONGO_DATABASE')['Parameter']['Value'],
        'MONGO_PORT': ssm_client.get_parameter(Name=f'/{env_name}/mongo/MONGO_PORT')['Parameter']['Value'],
        'MONGO_USERNAME': ssm_client.get_parameter(Name=f'/{env_name}/mongo/MONGO_USERNAME')['Parameter']['Value'],
        'MONGO_PASSWORD': ssm_client.get_parameter(Name=f'/{env_name}/mongo/MONGO_PASSWORD')['Parameter']['Value']
    }
    return secrets


def get_redshift_connection(ssm_client, env_name):
    """
    Function to get redshift connection details from AWS parameter store
    :param ssm_client: boto3 client to interact with aws parameter store
    :param env_name: stage/prod
    :return: dict with redshift creds
    """
    secrets = {
        'REDSHIFT_HOSTNAME': ssm_client.get_parameter(Name=f'/{env_name}/redshift/REDSHIFT_HOSTNAME')['Parameter'][
            'Value'],
        'REDSHIFT_LANDING_DATABASE':
            ssm_client.get_parameter(Name=f'/{env_name}/redshift/REDSHIFT_LANDING_DATABASE')['Parameter']['Value'],
        'REDSHIFT_DWH_DATABASE':
            ssm_client.get_parameter(Name=f'/{env_name}/redshift/REDSHIFT_DWH_DATABASE')['Parameter']['Value'],
        'REDSHIFT_PORT': ssm_client.get_parameter(Name=f'/{env_name}/redshift/REDSHIFT_PORT')['Parameter']['Value'],
        'REDSHIFT_USERNAME': ssm_client.get_parameter(Name=f'/{env_name}/redshift/REDSHIFT_USERNAME')['Parameter'][
            'Value'],
        'REDSHIFT_PASSWORD': ssm_client.get_parameter(Name=f'/{env_name}/redshift/REDSHIFT_PASSWORD')['Parameter'][
            'Value']
    }
    return secrets


def init_spark(env_name):
    """
    This function creates sparkSession and also gets mongo/redshift creds from aws parameter store
    :param env_name: stage/prod depending on this we have to get the creds from relevant env
    :return: dict with spark, mongo and redshift configurations if successful otherwise respective exceptions
    """
    try:
        region = eval(f"ConfigConst.{env_name.upper()}_AWS_REGION")
    except AttributeError:
        raise exceptions.UndefinedAWSRegion
    env_name = env_name.lower()
    ssm_client = boto3.client("ssm", region_name=region)
    mongo_secrets = get_mongo_connection(ssm_client, env_name)
    redshift_secrets = get_redshift_connection(ssm_client, env_name)
    mongo_connection_url = f"mongodb://{mongo_secrets['MONGO_USERNAME']}:{mongo_secrets['MONGO_PASSWORD']}@" \
                           f"{mongo_secrets['MONGO_HOSTNAME']}:{mongo_secrets['MONGO_PORT']}"
    conf = SparkConf()
    conf.set("spark.jars.packages",
             "org.mongodb.spark:mongo-spark-connector:10.0.3,com.amazon.redshift:redshift-jdbc42:2.1.0.1")
    conf.set("spark.mongodb.read.connection.uri", mongo_connection_url)
    SparkContext.getOrCreate(conf=conf).getOrCreate()
    sc = SparkSession.builder.appName('myApp').getOrCreate()
    spark_dict = {'spark_session': sc}
    return {'spark_config': spark_dict, 'mongo_config': mongo_secrets, 'redshift_config': redshift_secrets}
