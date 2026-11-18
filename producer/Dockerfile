FROM python:3.11-slim
RUN pip install --no-cache-dir confluent-kafka==2.6.1 psycopg2-binary==2.9.10
WORKDIR /app
COPY producer.py .
ENTRYPOINT ["python", "/app/producer.py"]
