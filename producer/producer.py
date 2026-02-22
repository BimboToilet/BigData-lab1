import os
import logging
from queue import Queue
import random
import time
import pandas as pd

from code.Producer import Producer

def load_dataset(path, logger):
    try:
        df = pd.read_csv(path)
        logger.info(f"Загрузка данных из {path}")
        return df
    except Exception as e:
        logger.error(f"Ошибка загрузки данных: {e}")
        raise

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    DATASET_PATH = os.getenv('DATASET_PATH', '/data/data1.csv')
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9092').split(',')
    OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'raw_data')
    PRODUCER_ID = os.getenv('PRODUCER_ID', '1')

    SLEEP_MIN = 0.01
    SLEEP_MAX = 0.1

    logger.info(f"Путь к данным: {DATASET_PATH}")
    logger.info(f"Брокеры: {BOOTSTRAP_SERVERS}")

    outcoming_messages = Queue()
    
    producer = Producer(logger, BOOTSTRAP_SERVERS, OUTPUT_TOPIC)
    producer_worker = producer.start(outcoming_messages)

    df = load_dataset(DATASET_PATH, logger)

    try:
        while True:
            for _, record in enumerate(df.to_dict(orient='records')):
                message = {
                    'timestamp': time.time(),
                    'producer_id': PRODUCER_ID,
                    'features': record,
                }
                outcoming_messages.put(message)
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            logger.info("Цикл отправки данных завершен, повтор цикла")
    except KeyboardInterrupt:
        logger.info("Отправка данных остановлена пользователем")
        
    producer.stop()
    producer_worker.join()

if __name__ == "__main__":
    main()