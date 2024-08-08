import argparse
import uuid
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer, SerializationContext, MessageField
from confluent_kafka import SerializingProducer, DeserializingConsumer

def consume_from_kafka(broker, schema_registry_url, topic, group_id):
    # 設置 Schema Registry 客戶端
    schema_registry_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 配置 Kafka 消費者
    avro_deserializer = AvroDeserializer(schema_registry_client)
    consumer_conf = {
        'bootstrap.servers': broker,
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    # 消費並顯示 Kafka 消息
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            record = msg.value()
            print(f"Consumed record with key {msg.key()} and value {record}")
        except KeyboardInterrupt:
            break

    consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Consume messages from a Kafka topic.')
    parser.add_argument('--broker', type=str, default='172.31.15.9:9092,172.31.13.152:9092,172.31.1.59:9092', help='Kafka broker address')
    parser.add_argument('--schema-registry', type=str, default='http://172.31.15.9:8081', help='Schema Registry URL')
    parser.add_argument('--topic', type=str, required=True, help='Kafka topic to consume from')
    parser.add_argument('--group-id', type=str, default=str(uuid.uuid4()), help='Consumer group ID')

    args = parser.parse_args()

    consume_from_kafka(args.broker, args.schema_registry, args.topic, args.group_id)

