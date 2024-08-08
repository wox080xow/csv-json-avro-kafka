import json
import fastavro
import avro.schema
import argparse
from decimal import Decimal
from io import BytesIO

# 定義 AVRO 架構
def read_avro_schema(schema_path):
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return schema

# 從 JSON 文件讀取資料
def read_json_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
        # 將 'c' 欄位轉換為 Decimal
        for record in data:
            record['c'] = Decimal(record['c'])
        return data

# 序列化 Decimal
def serialize_decimal(decimal_value, precision, scale):
    scale_factor = Decimal(10) ** -scale
    unscaled_value = int(decimal_value / scale_factor)
    return unscaled_value.to_bytes((unscaled_value.bit_length() + 7) // 8, byteorder='big', signed=True)

# 將 JSON 資料轉換為 AVRO 資料
def json_to_avro(json_data, schema, output_path):
    # 將 Decimal 欄位轉換為 bytes
    for record in json_data:
        record['c'] = serialize_decimal(record['c'], 10, 2)

    # 寫入 AVRO 文件
    with open(output_path, 'wb') as out:
        fastavro.writer(out, schema, json_data)

# 主程序
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert JSON to AVRO.')
    parser.add_argument('--json', type=str, required=True, help='Path to the JSON file')
    parser.add_argument('--schema', type=str, required=True, help='Path to the AVRO schema file')
    parser.add_argument('--output', type=str, required=True, help='Path to the output AVRO file')

    args = parser.parse_args()

    json_file_path = args.json
    avro_file_path = args.output
    schema_file_path = args.schema

    # 讀取 AVRO 架構
    schema = read_avro_schema(schema_file_path)

    # 讀取 JSON 資料
    json_data = read_json_data(json_file_path)
    
    # 將 JSON 資料轉換並寫入 AVRO 文件
    json_to_avro(json_data, schema, avro_file_path)

    print(f"Data has been written to {avro_file_path}")

