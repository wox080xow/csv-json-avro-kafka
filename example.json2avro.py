import json
import fastavro
from decimal import Decimal
from io import BytesIO

# 定義 AVRO 架構
schema = {
    "namespace": "test.avro",
    "type": "record",
    "name": "CSVDATA",
    "fields": [
        {"name": "a", "type": "int"},
        {"name": "b", "type": "int"},
        {"name": "c", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
    ]
}

# JSON 資料
json_data = [
    {"a": 1, "b": 2, "c": Decimal('12345.67')},
    {"a": 2, "b": 3, "c": Decimal('23456.78')}
]

# 將 JSON 資料轉換為 AVRO 資料
def serialize_decimal(decimal_value, precision, scale):
    # 將 Decimal 轉換為 bytes
    scale_factor = Decimal(10) ** -scale
    unscaled_value = int(decimal_value / scale_factor)
    return unscaled_value.to_bytes((unscaled_value.bit_length() + 7) // 8, byteorder='big', signed=True)

for record in json_data:
    record['c'] = serialize_decimal(record['c'], 10, 2)

# 寫入 AVRO 文件
output = BytesIO()
fastavro.writer(output, schema, json_data)
output.seek(0)

# 讀取 AVRO 文件以確認資料
avro_data = BytesIO(output.read())
avro_data.seek(0)
records = list(fastavro.reader(avro_data))

# 打印確認結果
for record in records:
    print(record)

