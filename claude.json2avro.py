import json
from decimal import Decimal
from fastavro import writer, parse_schema
import argparse

def convert_json_to_avro(input_file, output_file, schema_file):
    # 讀取JSON資料
    with open(input_file, 'r') as f:
        data = json.load(f)

    # 讀取AVRO模式
    with open(schema_file, 'r') as f:
        schema = json.load(f)

    # 將JSON數據轉換為AVRO
    parsed_schema = parse_schema(schema)
    with open(output_file, 'wb') as out:
        writer(out, parsed_schema, [
            {k: Decimal(str(v)) if k in ['SR_RETURN_AMT', 'SR_RETURN_TAX', 'SR_RETURN_AMT_INC_TAX', 'SR_FEE', 'SR_RETURN_SHIP_COST', 'SR_REFUNDED_CASH', 'SR_REVERSED_CHARGE', 'SR_STORE_CREDIT', 'SR_NET_LOSS'] else v
             for k, v in row.items()}
            for row in data
        ])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert JSON to AVRO')
    parser.add_argument('--input-file', required=True, help='Path to input JSON file')
    parser.add_argument('--output-file', required=True, help='Path to output AVRO file')
    parser.add_argument('--schema-file', required=True, help='Path to AVRO schema file')
    args = parser.parse_args()

    convert_json_to_avro(args.input_file, args.output_file, args.schema_file)
