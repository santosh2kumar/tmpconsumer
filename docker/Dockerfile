FROM python:3.6.8-slim

WORKDIR /app

RUN pip install -U numpy pandas requests redis-py-cluster kafka-python cassandra-driver confluent-kafka elasticsearch maprdb-python-client avro

COPY fraud_consumer_avro_es.py ./fraud_consumer_avro_es.py

ENTRYPOINT [ "python", "fraud_consumer_avro_es.py" ]

