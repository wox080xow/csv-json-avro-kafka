import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
from collections import namedtuple
from decimal import Decimal

def read_avro_data(path):
    with open(path, 'rb') as file:
        reader = DataFileReader(file, DatumReader())
        for record in reader:
            print(record)
        reader.close()

if __name__ == "__main__":
    # read_avro_data("CSVDATA.avro")
    # read_avro_data("test.avro")
    # read_avro_data("konber-test.avro")
    read_avro_data("fromjson.test.avro")

