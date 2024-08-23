import argparse
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import CachedSchemaRegistryClient
import configparser

# 讀取設定檔
config = configparser.ConfigParser()
config.read('config.ini')

def read_avro_data(path):
    with open(path, 'rb') as file:
        reader = DataFileReader(file, DatumReader())
        records = [record for record in reader]
        reader.close()
    return records

def main(topic, avro_file_path, schema_file_path):
    # Kafka and Schema Registry configuration
    #KAFKA_BROKER = '172.31.15.9:9092,172.31.13.152:9092,172.31.1.59:9092'
    KAFKA_BROKER = config['KAFKA']['KAFKA_BROKER']
    #SCHEMA_REGISTRY_URL = 'http://172.31.15.9:8081'
    SCHEMA_REGISTRY_URL = config['KAFKA']['SCHEMA_REGISTRY_URL']
    
    # 設置 Schema Registry 客戶端
    schema_registry_client = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)

    # 讀取 AVRO schema
    schema = avro.schema.parse(open(schema_file_path).read())

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
    records = read_avro_data(avro_file_path)

    for record in records:
        avro_producer.produce(topic=topic, value=record)
        avro_producer.flush()

    print("All records have been sent to Kafka.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send AVRO data to Kafka topic.')
    parser.add_argument('--topic', type=str, required=True, help='Kafka topic to send data to')
    parser.add_argument('--input', type=str, required=True, help='Path to the input AVRO file')
    parser.add_argument('--schema', type=str, required=True, help='Path to the AVRO schema file')

    args = parser.parse_args()

    main(args.topic, args.input, args.schema)

