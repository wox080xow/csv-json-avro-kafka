import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
import argparse
from collections import namedtuple
from decimal import Decimal

# 定義欄位和類型映射
fields = (
    "SR_RETURNED_DATE_SK", "SR_RETURN_TIME_SK", "SR_ITEM_SK", "SR_CUSTOMER_SK",
    "SR_CDEMO_SK", "SR_HDEMO_SK", "SR_ADDR_SK", "SR_STORE_SK", "SR_REASON_SK",
    "SR_TICKET_NUMBER", "SR_RETURN_QUANTITY", "SR_RETURN_AMT", "SR_RETURN_TAX",
    "SR_RETURN_AMT_INC_TAX", "SR_FEE", "SR_RETURN_SHIP_COST", "SR_REFUNDED_CASH",
    "SR_REVERSED_CHARGE", "SR_STORE_CREDIT", "SR_NET_LOSS"
)
CSVDATARecord = namedtuple('CSVDATARecord', fields)
type_mapping = {
    'SR_RETURNED_DATE_SK': int, 'SR_RETURN_TIME_SK': int, 'SR_ITEM_SK': int,
    'SR_CUSTOMER_SK': int, 'SR_CDEMO_SK': int, 'SR_HDEMO_SK': int,
    'SR_ADDR_SK': int, 'SR_STORE_SK': int, 'SR_REASON_SK': int,
    'SR_TICKET_NUMBER': int, 'SR_RETURN_QUANTITY': int, 'SR_RETURN_AMT': Decimal,
    'SR_RETURN_TAX': Decimal, 'SR_RETURN_AMT_INC_TAX': Decimal, 'SR_FEE': Decimal,
    'SR_RETURN_SHIP_COST': Decimal, 'SR_REFUNDED_CASH': Decimal, 'SR_REVERSED_CHARGE': Decimal,
    'SR_STORE_CREDIT': Decimal, 'SR_NET_LOSS': Decimal
}

def convert_value(field, value):
    if value == '(null)' or value == '' or value is None:
        if type_mapping[field] == int:
            return 0
        elif type_mapping[field] == Decimal:
            return Decimal(0)
    return type_mapping[field](value)

def read_CSVDATA_data(path):
    with open(path, 'r') as data:
        data.readline()  # 跳過標頭行
        reader = csv.reader(data, delimiter="\t")  # 假設資料是用 Tab 分隔的
        for row in reader:
            # converted_row = [type_mapping[field](value) for field, value in zip(fields, row)]
            converted_row = [convert_value(field, value) for field, value in zip(fields, row)]
            yield CSVDATARecord._make(converted_row)

def parse_schema(path):
    with open(path, 'r') as data:
        schema_json = data.read()
        return avro.schema.parse(schema_json)

def serialize_records(records, schema_path, outpath):
    schema = parse_schema(schema_path)
    with open(outpath, 'wb') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            record_dict = dict((f, getattr(record, f)) for f in record._fields)
            writer.append(record_dict)
        writer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert CSV data to Avro format.')
    parser.add_argument('--input', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--schema', type=str, required=True, help='Path to the Avro schema file')
    parser.add_argument('--output', type=str, required=True, help='Path to the output Avro file')

    args = parser.parse_args()

    # 序列化記錄並寫入 Avro 文件
    serialize_records(read_CSVDATA_data(args.input), args.schema, args.output)

