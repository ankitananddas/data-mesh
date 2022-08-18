"""
This file contains constants like configuration values, error message and
success messages and to be used across the project
"""


class ConfigConst:
    """
    This class will contain the list of all the config params and default values
    """
    DEF_FB_LIMIT = 10
    DEFAULT_INTEGER = 0
    DEFAULT_FLOAT = 0.0
    DEFAULT_STRING = "0"
    EMPTY_STRING = ""
    STAGE_AWS_REGION = "us-west-2"  # TODO: create respective variable for prod when finalized


class MessagesConst:
    """
    This class will contain all the messages, both success and error messages
    """
    COMMON_ERR_MSG = {
        "GENERIC_ERROR": "Error while running the data population script",
        "UNDEFINED": "Can not find the error corresponding to the specified error code"
    }

    CONFIG_ERR_MSG = {
        "UNDEFINED_AWS_REGION": "Can not find the AWS region to the specified ENV",
    }

    CONNECTION_FAILURE_ERR_MSG = {
        "REDSHIFT_CONNECTION_FAILURE": "Failure while connecting to redshift",
        "MONGO_CONNECTION_FAILURE": "Failure while connecting to mongo",
        "S3_CONNECTION_FAILURE": "Failure while connecting to S3",
    }

    DATA_READ_FAILURE_ERR_MSG = {
        "REDSHIFT_READ_FAILURE": "Failure while reading to redshift",
        "MONGO_READ_FAILURE": "Failure while reading to mongo",
        "S3_READ_FAILURE": "Failure while reading to S3",
    }

    DATA_WRITE_FAILURE_ERR_MSG = {
        "UNKNOWN_SPARK_WRITE_MODE": "Write mode for spark should be either append or overwrite",
        "REDSHIFT_WRITE_FAILURE": "Failure while writing to redshift",
        "MONGO_WRITE_FAILURE": "Failure while writing to mongo",
        "S3_WRITE_FAILURE": "Failure while writing to S3",
    }

    DATA_TRANSFORMATION_ERR_MSG = {}

    DATA_FLATTENING_ERR_MSG = {
        "IS_INVALID_KEY_MISSING": "invalid key missing in array of struct containing email/phone data",
        "IS_PRIMARY_KEY_MISSING": "isPrimary key missing in array of struct containing email/phone data"
    }
