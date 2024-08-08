import json
import fastavro
import argparse
from decimal import Decimal, getcontext

# 設置 Decimal 的精度，以防止溢出
getcontext().prec = 50

# 定義 AVRO 架構
def read_avro_schema(schema_path):
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return schema

# 從 JSON 文件讀取資料
def read_json_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

# 序列化 Decimal
def serialize_decimal(decimal_value, scale):
    # 將小數點移動，生成未縮放的值
    unscaled_value = int(decimal_value * (10 ** scale))
    # num_bytes = (unscaled_value.bit_length() + 7) // 8
    num_bytes = 4
    return unscaled_value.to_bytes(num_bytes, byteorder='big', signed=True)

# 將 JSON 資料轉換為 AVRO 資料
def json_to_avro(json_data, schema, output_path):
    # 獲取需要處理的欄位名稱及其屬性
    decimal_fields = [(field['name'], field['type']['precision'], field['type']['scale']) for field in schema['fields'] if isinstance(field['type'], dict) and field['type'].get('logicalType') == 'decimal']
    
    # 處理 Decimal 欄位
    for record in json_data:
        for field, precision, scale in decimal_fields:
            print(record[field])
            record[field] = serialize_decimal(Decimal(record[field]), scale)

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

