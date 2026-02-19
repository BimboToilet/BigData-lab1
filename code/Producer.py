from queue import Queue
import threading
import time
import json
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


class Producer:
    def __init__(self, logger: logging.Logger, bootstrap_servers: list, output_topic: str):
        self.logger = logger
        self.output_topic = output_topic
        self.bootstrap_servers = bootstrap_servers
        self.running = False
    
    def __create_producer(self, retries = 5):
        for i in range(retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                )
                self.logger.info("Producer создан")
                return
            except NoBrokersAvailable:
                self.logger.warning(f"Не найдены доступные брокеры, попытка: {i+1}/{retries}")
                time.sleep(5)
        raise Exception("Не удалось связаться с брокерами")
    
    def on_success(self, metadata):
        self.logger.debug(f"Доставлено: {metadata.topic} [{metadata.partition}]")

    def on_error(self, exc):
        self.logger.error(f"Ошибка при доставке: {exc}")

    def start(self, outcoming_messages):
        self.producer = self.__create_producer()
        thread = threading.Thread(
            target=self.work, 
            args=(outcoming_messages), 
            daemon=False
        )
        self.running = True
        thread.start()
        return thread
    
    def stop(self):
        self.running = False
    
    def work(self, outcoming_messages: Queue):
        try:
            while self.running:
                message = outcoming_messages.get()
                self.producer.send(self.output_topic, value=message).add_callback(self.on_success).add_errback(self.on_error)
                self.logger.info("Сообщение отправлено")
                self.producer.flush()
        except Exception as e:
            self.logger.error(f"Ошибка при работе Producer: {e}")
        finally:
            self.producer.close()
