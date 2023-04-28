from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from elasticsearch import Elasticsearch
import pandas as pd
import requests, json, time
from datetime import datetime
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

headers = {
    'Content-Type': "application/json"
    }

c = AvroConsumer({
    'bootstrap.servers': 'idp-kf-cp-kafka-headless:9092',
    'group.id': 'groupid',
    'schema.registry.url': 'http://idp-sc-cp-schema-registry:8081'})

c.subscribe(['fraud_avro1'])

es = Elasticsearch("http://elasticsearch.default.svc:9200")

connection_str = "10.0.15.51:5678?auth=basic;user=mapr;password=mapr;ssl=true;sslCA=/mnt/ssl_truststore.pem;sslTargetNameOverride=edf1.isv.local"

connection = ConnectionFactory.get_connection(connection_str=connection_str)

try:
    store = connection.get_store('/fraud_prediction_data')
except:
    store = connection.create_store('/fraud_prediction_data')


while True:
    try:
        msg = c.poll(1)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue
    
    data = msg.value()
    id = msg.key()['transaction_cnt']

    now = datetime.now()
    recorded_date = time = now.strftime("%Y-%m-%d")
    recorded_time = time = now.strftime("%H:%M:%S")

    mapr_data = data.copy()
    elastic_data = data.copy()
    mapr_data['_id'] = str(id) + "_" + recorded_date + "_" + recorded_time
    new_document = connection.new_document(dictionary=mapr_data)
    store.insert_or_replace(new_document)

    data['step_day'] = data['step'] // 24
    data['hour'] = data['step'] % 24
    data['step_week'] = data['step'] // 24

    data['CASH_OUT'] = 0
    data['DEBIT'] = 0
    data['PAYMENT'] = 0
    data['TRANSFER'] = 0
  
    if(data['type'] != 'CASH_IN'):
        data[data['type']] = 1
    else:
        print("CASH_IN Transaction")

    data['CM'] = 0

    if(data['nameOrig'][0] + data['nameDest'][0] == 'CM'):
        data['CM'] = 1
    else:
        print("CC Transaction")

    data['errorOrig'] = data['amount'] + data['newBalanceOrig'] - data['oldBalanceOrig']
    data['errorDest'] = data['amount'] + data['oldBalanceDest'] - data['newBalanceDest']

    nameOrig = data.pop('nameOrig')
    nameDest = data.pop('nameDest')
    type = data.pop('type')
    value = data.pop('step')
    value = data.pop('isFraud')
    value = data.pop('isFlaggedFraud')

    response = requests.request("POST" , "http://fraud-detect-ep:5000/api/v0/verify", headers=headers, data=json.dumps(data))
    resp_data = json.loads(response.text)[0]
    data['Fraud_Prediction'] = 'Genuine' if(resp_data['prediction']=='0') else 'Fraud'

    elastic_data['Fraud_Prediction'] = data['Fraud_Prediction']
    elastic_data['recorded_datetime'] = now
    res = es.index(index="transactions", id=str(id) + "_" + recorded_date + "_" + recorded_time, document=elastic_data)
    print(elastic_data)
