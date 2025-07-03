# Use pip to install packages missing from conda

$PREFIX/bin/pip install \
'rocksdict>=0.3,<0.4' \
'protobuf>=5.27.2,<7.0' \
'influxdb3-python[pandas]>=0.7,<1.0' \
'pyiceberg[pyarrow,glue]>=0.7' \
'redis[hiredis]>=5.2.0,<6' \
'confluent-kafka[avro,json,protobuf,schemaregistry]>=2.8.2,<2.10' \
'influxdb>=5.3,<6' \
'jsonpath_ng>=1.7.0,<2' \
'types-psycopg2>=2.9,<3' \
'paho-mqtt>=2.1.0,<3'
