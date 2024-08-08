from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer, SerializationContext, MessageField
from confluent_kafka import SerializingProducer, DeserializingConsumer

# Kafka and Schema Registry configuration
KAFKA_BROKER = '172.31.15.9:9092,172.31.13.152:9092,172.31.1.59:9092'
SCHEMA_REGISTRY_URL = 'http://172.31.15.9:8081'
TOPIC = 'test'

# 設置 Schema Registry 客戶端
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 配置 Kafka 消費者
avro_deserializer = AvroDeserializer(schema_registry_client)
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'group.id': 'test_group',
    'auto.offset.reset': 'earliest'
}
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([TOPIC])

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
