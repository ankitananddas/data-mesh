from .constants import common as const
from .tables import campaigns
from .utils import save_data


def populate_landing_layer(spark, mongo_creds, redshift_creds, logger):
    df = campaigns.load_campaign_data_into_landing_layer(spark, mongo_creds, redshift_creds, logger)
    save_data.store_into_redshift(spark, df, 'campaign', 'append')
    logger.info("Job Completed")
