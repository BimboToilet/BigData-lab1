from queue import Queue
import threading
import time
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


class Consumer:
    def __init__(self, logger: logging.Logger, bootstrap_servers: list, input_topic: str):
        self.logger = logger
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.running = False
    
    def __create_consumer(self, retries = 5):
        for i in range(retries):
            try:
                self.consumer = KafkaConsumer(
                    self.input_topic,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    max_poll_records=100
                )
                self.logger.info("Consumer создан")
                return
            except NoBrokersAvailable:
                self.logger.warning(f"Не найдены доступные брокеры, попытка: {i+1}/{retries}")
                time.sleep(5)
        raise Exception("Не удалось связаться с брокерами")
    
    def start(self, incoming_messages):
        self.consumer = self.__create_consumer()
        thread = threading.Thread(
            target=self.work, 
            args=(incoming_messages), 
            daemon=False
        )
        self.running = True
        thread.start()
        return thread
    
    def stop(self):
        self.running = False
    
    def work(self, incoming_messages: Queue):
        try:
            for message in self.consumer:
                try:
                    msg_value = message.value
                    self.logger.debug(f"Получено сообщение: {msg_value}")
                    incoming_messages.put(msg_value)
                    if not self.running:
                        break
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке сообщения: {e}")
                    continue
        except Exception as e:
            self.logger.error(f"Ошибка при чтении сообщений: {e}")
        finally:
            self.consumer.close()
