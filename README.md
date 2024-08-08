```
python3 consume.py --topic json-test

python3 json2avro.py --json test.json --schema test.avsc --output 2.fromjson.test.avro

python3 claude.json2avro.py --input-file 10.store_returns7M.json --output-file claude.JSONDATA.avro --schema-file DATA.avsc

python3 csv2avro.py --input 10.store_returns7M.csv --schema DATA.avsc --output CSVDATA.avro

python3 read_and_produce.py --topic test-2 --input test.avro --schema test.avsc
```
