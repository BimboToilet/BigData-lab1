import os
from queue import Queue
import time
import pickle
import logging
from datetime import datetime
from sklearn.preprocessing import StandardScaler
import pandas as pd

from code.Consumer import Consumer
from code.Producer import Producer

def load_scaler(path, logger):
    try:
        with open(path, 'rb') as f:
            scaler = pickle.load(f)
        logger.info(f"Веса масштаба загружены из {path}")
        return scaler
    except Exception as e:
        logger.error(f"Ошибка при загрузке весов масштаба: {e}")
        raise

def extract_hour(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    return dt.hour

def process_message(message_value, scaler, logger):
    try:
        idx = message_value.get('record_id')
        features = message_value.get('features')

        if features is None:
            raise ValueError("Нет признаков")
        
        features['Hour'] = extract_hour(features['date'])

        features_row = pd.DataFrame([features]).drop(['date'], axis = 1)

        normalized_row = pd.DataFrame(scaler.transform(features_row), columns=features_row.columns)

        features = normalized_row.to_dict(orient='records')[0]

        processed = {
            'timestamp': time.time(),
            'record_id': idx,
            'features': features
        }

        return processed
    except Exception as e:
        logger.error(f"Ошибка во время препроцессинга сообщения: {e}, сообщение: {message_value}")
        return None

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9092').split(',')
    INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'raw_data')
    OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'raw_preprocessed')
    SCALER_PATH = os.getenv('SCALER_PATH', '/scaler/scaler.pkl')

    logger.info(f"Брокеры: {BOOTSTRAP_SERVERS}")
    logger.info(f"Источники данных: {INPUT_TOPIC}, вывод данных: {OUTPUT_TOPIC}")
    logger.info(f"Веса масштабирования: {SCALER_PATH}")

    scaler = load_scaler(SCALER_PATH, logger)

    incoming_messages = Queue()
    outcoming_messages = Queue()

    consumer = Consumer(logger, BOOTSTRAP_SERVERS, INPUT_TOPIC)
    consumer_worker = consumer.start(incoming_messages)
    producer = Producer(logger, BOOTSTRAP_SERVERS, OUTPUT_TOPIC)
    producer_worker = producer.start(outcoming_messages)

    try:
        while True:
            message = incoming_messages.get()
            message = process_message(message, scaler, logger)
            outcoming_messages.put(message)
    except KeyboardInterrupt:
        logger.info("Препроцессинг данных остановлен пользователем")

    producer.stop()
    producer_worker.join()
    consumer.stop()
    consumer_worker.join()

if __name__ == "__main__":
    main()