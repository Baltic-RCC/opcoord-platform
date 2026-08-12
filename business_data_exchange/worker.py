import sys

import config
from loguru import logger
from settings import WorkerSettings
from business_data_exchange.handlers import BusinessDataExchangeHandler

conf = WorkerSettings()

# Set worker name and unique id to Elastic log handler
if getattr(config.init_log_handlers.conf, 'elastic_handler', None):
    config.init_log_handlers.elastic_handler.extra.update({'worker': conf.worker_name, 'worker_id': conf.worker_id})

# Run-once implementation: scheduled externally as a cronjob once per day,
# before 1D CROSA and after the CSA consistency check process
logger.info(f"Starting 'business-data-exchange' worker with assigned id: {conf.worker_id}")
try:
    handler = BusinessDataExchangeHandler()
    handler.handle()
except Exception as e:
    logger.exception(f"Business data exchange process failed: {e}")
    sys.exit(1)
