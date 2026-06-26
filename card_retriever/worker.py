import config
from integrations import rmq
from card_retriever.handlers import RootRetrievingHandler
from loguru import logger
from settings import WorkerSettings

conf = WorkerSettings()

# Set worker name and unique id to Elastic log handler
if getattr(config.init_log_handlers.conf, 'elastic_handler', None):
    config.init_log_handlers.elastic_handler.extra.update({'worker': conf.worker_name, 'worker_id': conf.worker_id})

# RabbitMQ consumer implementation
logger.info(f"Starting 'card-retriever' worker with assigned id: {conf.worker_id}")
handlers = [RootRetrievingHandler()]
consumer = rmq.RMQConsumer(
    queue=conf.rmq_queue_in,
    message_handlers=handlers,
)
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
