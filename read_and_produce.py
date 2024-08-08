import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
from collections import namedtuple
from decimal import Decimal

from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import CachedSchemaRegistryClient

def read_avro_data(path):
    with open(path, 'rb') as file:
        reader = DataFileReader(file, DatumReader())
        records = [record for record in reader]
        reader.close()
    return records

# Kafka and Schema Registry configuration
KAFKA_BROKER = '172.31.15.9:9092,172.31.13.152:9092,172.31.1.59:9092'
SCHEMA_REGISTRY_URL = 'http://172.31.15.9:8081'
#TOPIC = 'CSVDATA2AVRO'
TOPIC = 'test'

# 設置 Schema Registry 客戶端
schema_registry_client = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)

# 讀取AVRO schema
schema_path = "test.avsc"
schema = avro.schema.parse(open(schema_path).read())

# 配置 Kafka 生產者
avro_producer = AvroProducer(
    {
        'bootstrap.servers': KAFKA_BROKER,
        'on_delivery': lambda err, msg: print('Message delivered' if err is None else f'Message delivery failed: {err}')
    },
    schema_registry=schema_registry_client,
    default_key_schema=None,
    default_value_schema=schema
)

# 讀取 AVRO 文件並發送到 Kafka
avro_file_path = "test.avro"
records = read_avro_data(avro_file_path)

for record in records:
    avro_producer.produce(topic=TOPIC, value=record)
    avro_producer.flush()

print("All records have been sent to Kafka.")

