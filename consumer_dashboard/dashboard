FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard.py ./

EXPOSE 8501

ENV STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

ENTRYPOINT ["streamlit", "run", "dashboard.py"]
CMD ["--server.port=8501", "--server.address=0.0.0.0"]