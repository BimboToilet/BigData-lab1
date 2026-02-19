import os
from queue import Queue
import queue
import time
import logging
import pandas as pd
import streamlit as st
import plotly.express as px

from BigData.Lab1.code.Consumer import Consumer

def process_message(message_value, logger):
    try:
        features = message_value.get('features')
        message_value.pop('features')
        processed = pd.DataFrame.from_dict(message_value | features)
        return processed
    except Exception as e:
        logger.error(f"Ошибка во время инференса модели: {e}, сообщение: {message_value}")
        return None

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9092').split(',')
    INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'ready_data')

    logger.info(f"Брокеры: {BOOTSTRAP_SERVERS}")
    logger.info(f"Источники данных: {INPUT_TOPIC}")

    incoming_messages = Queue()

    st.set_page_config(page_title="Kafka ML Dashboard", layout="wide")
    st.title("Real-time Kafka ML Dashboard")
    st.markdown("Мониторинг потока предсказаний модели классификации о том, есть ли люди в помещении или нет")

    if 'consumer_started' not in st.session_state:
        with st.spinner("Запуск Kafka Consumer..."):
            consumer = Consumer(logger, [BOOTSTRAP_SERVERS], INPUT_TOPIC)
            consumer_worker = consumer.start(incoming_messages)
            st.session_state.consumer_started = True
            st.session_state.messages = []
            st.success("Consumer запущен")

    st.sidebar.header("Настройки отображения")
    max_messages = st.sidebar.slider("Максимум сообщений на графиках", 10, 500, 200)
    refresh_interval = st.sidebar.slider("Интервал обновления, сек", 1, 10, 2)

    placeholder = st.empty()

    try:
        while True:
            message_count = 0
            while not incoming_messages.empty():
                try:
                    msg = incoming_messages.get_nowait()
                    msg = process_message(msg, logger)
                    st.session_state.messages.append(msg)
                    new_count += 1
                except queue.Empty:
                    break

            if message_count > 0:
                st.sidebar.info(f"Получено новых сообщений: {message_count}")

            if len(st.session_state.messages) > max_messages:
                st.session_state.messages = st.session_state.messages[-max_messages:]

            if st.session_state.messages:
                df = pd.DataFrame(st.session_state.messages)

                if 'timestamp' in df.columns:
                    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
                else:
                    df['datetime'] = pd.Timestamp.now()

                total_msgs = len(st.session_state.messages)
                unique_preds = df['label'].nunique() if 'label' in df else 0

                with placeholder.container():
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Всего в буфере", total_msgs)
                    col2.metric("Классов предсказаний", unique_preds)
                    col3.metric("Топик", INPUT_TOPIC)

                    st.markdown("---")

                    col_left, col_right = st.columns(2)

                    with col_left:
                        st.subheader("Распределение предсказаний")
                        if 'label' in df.columns:
                            fig_pie = px.pie(df, names='label', title='Классы предсказаний')
                            st.plotly_chart(fig_pie, use_container_width=True)
                        else:
                            st.warning("Нет данных о предсказаниях")

                    with col_right:
                        st.subheader("Динамика предсказаний во времени")
                        if 'datetime' in df.columns and 'label' in df.columns:
                            fig_scatter = px.scatter(
                                df, x='datetime', y='label', title='Предсказания по времени',
                                labels={'label': 'Класс'}
                            )
                            st.plotly_chart(fig_scatter, use_container_width=True)
                        else:
                            st.warning("Недостаточно данных для временного ряда")

                    st.markdown("---")

                    st.subheader(f"Последние {len(df)} записей")
                    cols_to_show = df.columns
                    st.dataframe(
                        df[cols_to_show].sort_values('datetime', ascending=False).head(20),
                        use_container_width=True
                    )
            else:
                with placeholder.container():
                    st.info("Ожидание данных...")

            time.sleep(refresh_interval)
    except KeyboardInterrupt:
        logger.info("Отображение данных остановлено пользователем")

    consumer.stop()
    consumer_worker.join()

if __name__ == "__main__":
    main()
