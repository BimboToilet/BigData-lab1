import os
from queue import Queue
import queue
import time
import logging
import pandas as pd
import streamlit as st
import plotly.express as px

from code.Consumer import Consumer

def process_message(message_value, logger):
    try:
        features = message_value.get('features')
        message_value.pop('features')
        processed = message_value | features
        return processed
    except Exception as e:
        logger.error(f"Ошибка во время обработки сообщения: {e}, сообщение: {message_value}")
        return None

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9092').split(',')
    INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'ready_data')
    
    st.set_page_config(page_title="Kafka ML Dashboard", layout="wide")
    st.sidebar.header("Настройки отображения")
    max_messages = st.sidebar.slider("Максимум сообщений на графиках", 10, 500, 200)
    refresh_interval = st.sidebar.slider("Интервал обновления, сек", 1, 10, 2)

    if 'incoming_messages' not in st.session_state:
        logger.info(f"Брокеры: {BOOTSTRAP_SERVERS}")
        logger.info(f"Источники данных: {INPUT_TOPIC}")
    
        st.session_state.incoming_messages = Queue()
        st.session_state.raw_messages = []
        
        with st.spinner("Запуск Kafka Consumer..."):
            consumer = Consumer(logger, BOOTSTRAP_SERVERS, INPUT_TOPIC)
            st.session_state.consumer_worker = consumer.start(st.session_state.incoming_messages)
            st.session_state.consumer_obj = consumer 
            st.success("Consumer запущен")

    st.title("Real-time Kafka ML Dashboard")

    @st.fragment(run_every=refresh_interval)
    def update_dashboard():
        st.markdown("Мониторинг потока предсказаний модели классификации о том, есть ли люди в помещении или нет")

        container = st.container()

        incoming_queue = st.session_state.incoming_messages
        message_count = 0
        
        while not incoming_queue.empty():
            try:
                msg = incoming_queue.get_nowait()
                msg = process_message(msg, logger)
                if msg:
                    msg['received_at'] = pd.Timestamp.now()
                    st.session_state.raw_messages.append(msg)
                    message_count += 1
            except queue.Empty:
                break

        if message_count > 0:
            st.toast(f"Получено новых сообщений: {message_count}")

        if len(st.session_state.raw_messages) > max_messages:
            st.session_state.raw_messages = st.session_state.raw_messages[-max_messages:]

        if st.session_state.raw_messages:
            df = pd.DataFrame(st.session_state.raw_messages)

            if 'date' not in df.columns:
                df['date'] = pd.Timestamp.now()

            total_msgs = len(st.session_state.raw_messages)
            unique_preds = df['label'].nunique() if 'label' in df else 0

            with container:
                col1, col2, col3 = st.columns(3)
                col1.metric("Всего в буфере", total_msgs)
                col2.metric("Классов предсказаний", unique_preds)
                col3.metric("Топик", INPUT_TOPIC)

                st.markdown("---")

                col_left, col_middle, col_right = st.columns(3)

                with col_left:
                    st.subheader("Распределение предсказаний")
                    if 'label' in df.columns:
                        fig_pie = px.pie(df, names='label', title='Классы предсказаний')
                        st.plotly_chart(fig_pie, key = "labels_chart")
                    else:
                        st.warning("Нет данных о предсказаниях")

                with col_middle:
                    st.subheader("Распределение producer")
                    if 'producer_id' in df.columns:
                        fig_pie = px.pie(df, names='producer_id', title='Producer ID')
                        st.plotly_chart(fig_pie, key = "producers_chart")
                    else:
                        st.warning("Нет данных о producer")

                with col_right:
                    st.subheader("Динамика предсказаний во времени")
                    if 'date' in df.columns and 'probability' in df.columns:
                        fig_scatter = px.scatter(
                            df, x='date', y='probability', title='Вероятности по времени',
                            labels={'probability': 'Вероятность', 'date': 'Время'}
                        )
                        st.plotly_chart(fig_scatter, key = "proba_chart")
                    else:
                        st.warning("Недостаточно данных для временного ряда")

                st.markdown("---")

                st.markdown("Интенсивность сообщений в секунду")
                if 'received_at' in df.columns:
                    df['received_at'] = pd.to_datetime(df['received_at'])
                    df['second'] = df['received_at'].dt.floor('s')
                    intensity_df = df.groupby('second').size().reset_index(name='count')
                    fig_intensity = px.bar(
                        intensity_df,
                        x='second',
                        y='count',
                        title='Количество сообщений в секунду',
                        labels={'second': 'Время', 'count': 'Сообщений в секунду'}
                    )
                    st.plotly_chart(fig_intensity, key = "intensity_chart")
                else:
                    st.warning("Нет данных о времени получения сообщений")

                st.subheader(f"Последние {len(df)} записей")
                exclude_cols = ['received_at', 'second']
                cols_to_show = [col for col in df.columns if col not in exclude_cols]
                st.dataframe(
                    df[cols_to_show].sort_values('date', ascending=False).head(20),
                    width='stretch'
                )
        else:
            with container:
                st.info("Ожидание данных...")

    update_dashboard()

if __name__ == "__main__":
    main()
