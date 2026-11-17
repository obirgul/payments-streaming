FROM python:3.11-slim
RUN pip install --no-cache-dir confluent-kafka==2.6.1
WORKDIR /app
COPY producer.py .
ENTRYPOINT ["python", "/app/producer.py"]
