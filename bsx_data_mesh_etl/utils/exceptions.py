"""
This module contains the custom exception classes which are used in this application.
"""
from ..constants.common import MessagesConst


class BasixException(Exception):
    """
    This is the generic exception to be used in case we are not sure which specific exception to use
    """

    def __init__(self, error_type="COMMON", error_code="GENERIC_ERROR") -> None:
        """
        :param error_type: Type of the error to search name of dictionary in constants
        :param error_code: Error code to get the error description
        """
        self.error_type = error_type
        self.error_code = error_code
        try:
            self.err_msg = eval(f"MessagesConst.{error_type}_ERR_MSG[error_code]")
        except:
            self.error_type = "COMMON"
            self.error_code = "UNDEFINED"
            self.err_msg = eval(f"MessagesConst.{error_type}_ERR_MSG[error_code]")
        finally:
            super().__init__(self.error_type, self.error_code, self.err_msg)

    def __str__(self) -> str:
        return super().__str__()


class UnknownSparkWriteModeException(BasixException):
    """
    This exception should be raised when the write mode for spark df is neither overwrite nor append
    """

    def __init__(self, error_type="DATA_WRITE_FAILURE", error_code="UNKNOWN_SPARK_WRITE_MODE") -> None:
        super(UnknownSparkWriteModeException, self).__init__(error_type, error_code)


class UndefinedAWSRegion(BasixException):
    """
    This exception should be raised when the AWS region is not defined for some ENV
    """

    def __init__(self, error_type="CONFIG", error_code="UNDEFINED_AWS_REGION") -> None:
        super(UndefinedAWSRegion, self).__init__(error_type, error_code)


class MongoReadException:
    """
    This exception should be raised when there are exceptions while reading data from mongo
    """

    def __init__(self, error_type="DATA_READ_FAILURE", error_code="MONGO_READ_FAILURE") -> None:
        super(MongoReadException, self).__init__(error_type, error_code)


class RedshiftReadException:
    """
    This exception should be raised when there are exceptions while reading data from redshift
    """

    def __init__(self, error_type="DATA_READ_FAILURE", error_code="REDSHIFT_READ_FAILURE") -> None:
        super(RedshiftReadException, self).__init__(error_type, error_code)


class RedshiftConnectionException(BasixException):
    """
    This exception should be raised when there are exceptions while connecting to redshift
    """

    def __init__(self, error_type="CONNECTION_FAILURE", error_code="REDSHIFT_CONNECTION_FAILURE") -> None:
        super(RedshiftConnectionException, self).__init__(error_type, error_code)


class RedshiftWriteException(BasixException):
    """
    This exception should be raised when there are exceptions while writing data to redshift
    """

    def __init__(self, error_type="DATA_WRITE_FAILURE", error_code="REDSHIFT_WRITE_FAILURE") -> None:
        super(RedshiftWriteException, self).__init__(error_type, error_code)


class FlatteningArrayOfStructIsPrimaryKeyMissing(BasixException):
    """
    This exception should be raised when isPrimary key missing in flattening array of struct containing email/phone data
    """

    def __init__(self, error_type="DATA_FLATTENING_ERR_MSG", error_code="IS_PRIMARY_KEY_MISSING") -> None:
        super(FlatteningArrayOfStructIsPrimaryKeyMissing, self).__init__(error_type, error_code)


class FlatteningArrayOfStructIsInvalidKeyMissing(BasixException):
    """
    This exception should be raised when invalid key missing in flattening array of struct containing email/phone data
    """

    def __init__(self, error_type="DATA_FLATTENING_ERR_MSG", error_code="IS_INVALID_KEY_MISSING") -> None:
        super(FlatteningArrayOfStructIsInvalidKeyMissing, self).__init__(error_type, error_code)
