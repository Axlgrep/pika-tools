#!/bin/bash
#author justforfun
cd ../third/redis-rdb-tools
sudo python setup.py install
cd ../../rdb_to_pika

rdb_filename=dump.rdb
if [ $# != 0 ]
then
rdb_filename=$1
fi
echo "${rdb_filename}"
rdb -c protocol -f protocol_file  ${rdb_filename}

