import os
from queue import Queue
import time
import pickle
import logging
from sklearn.ensemble import RandomForestClassifier
import pandas as pd

from code.Consumer import Consumer
from code.Producer import Producer

def load_model(path, logger):
    try:
        with open(path, 'rb') as f:
            model = pickle.load(f)
        logger.info(f"Модель загружена из {path}")
        return model
    except Exception as e:
        logger.error(f"Ошибка при загрузке модели: {e}")
        raise

def process_message(message_value, model, logger):
    try:
        idx = message_value.get('record_id')
        features = message_value.get('features')

        if features is None:
            raise ValueError("Нет признаков")

        features_row = pd.DataFrame([features])

        label = model.predict(features_row)
        
        probability = model.predict_proba(features_row)[1]

        processed = {
            'timestamp': time.time(),
            'record_id': idx,
            'label': label,
            'probability': probability,
            'features': features
        }

        return processed
    except Exception as e:
        logger.error(f"Ошибка во время инференса модели: {e}, сообщение: {message_value}")
        return None

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9092').split(',')
    INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'raw_preprocessed')
    OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'ready_data')
    MODEL_PATH = os.getenv('MODEL_PATH', '/model/model.pkl')

    logger.info(f"Брокеры: {BOOTSTRAP_SERVERS}")
    logger.info(f"Источники данных: {INPUT_TOPIC}, вывод данных: {OUTPUT_TOPIC}")
    logger.info(f"Веса модели: {MODEL_PATH}")

    model = load_model(MODEL_PATH, logger)

    incoming_messages = Queue()
    outcoming_messages = Queue()

    consumer = Consumer(logger, BOOTSTRAP_SERVERS, INPUT_TOPIC)
    consumer_worker = consumer.start(incoming_messages)
    producer = Producer(logger, BOOTSTRAP_SERVERS, OUTPUT_TOPIC)
    producer_worker = producer.start(outcoming_messages)

    try:
        while True:
            message = incoming_messages.get()
            message = process_message(message, model, logger)
            outcoming_messages.put(message)
    except KeyboardInterrupt:
        logger.info("Инференс на данных остановлен пользователем")

    producer.stop()
    producer_worker.join()
    consumer.stop()
    consumer_worker.join()

if __name__ == "__main__":
    main()