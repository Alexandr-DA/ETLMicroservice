import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from dds_loader.repository.dds_repository import DdsRepository
from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor

app = Flask(__name__)

config = AppConfig()


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    proc = DdsMessageProcessor(consumer=config.kafka_consumer(), 
								producer=config.kafka_producer(),
								dds_repository=DdsRepository(config.pg_warehouse_db()),
								logger=app.logger)

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
