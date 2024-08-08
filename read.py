import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import argparse

def read_avro_data(path):
    with open(path, 'rb') as file:
        reader = DataFileReader(file, DatumReader())
        for record in reader:
            print(record)
        reader.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read AVRO file and print records.')
    parser.add_argument('avro_file', type=str, help='Path to the AVRO file to read')

    args = parser.parse_args()
    read_avro_data(args.avro_file)

