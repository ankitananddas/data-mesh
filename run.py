from datetime import datetime
import logging
from bsx_data_mesh_etl.utils import init_spark
from bsx_data_mesh_etl import populate_tables as driver_script


# For Local Run
init_config = init_spark.init_spark('stage')
redshift_creds = init_config['mongo_config']
mongo_creds = init_config['mongo_config']
spark = init_config['spark_config']['spark_session']
logger = logging.getLogger()
logger.setLevel(10)


# run the script for populating the landing layer
driver_script.populate_landing_layer(spark, mongo_creds, redshift_creds, logger)

