1. go get -u github.com/Shopify/sarama
2. go build -buildmode=c-archive -o kafka_broker.a
3. copy kafka_broker.a and pg_recvlogical.c Makefile PG_SRC/bin/pg_basebackup/
4. cd PG_SRC/bin/pg_basebackup/
5. make
6. ./pg_recvlogical -U postgres -w -d dbname --slot test_slot --start -o include-xids=1 -o include-lsn=1 -f - -b "kafka://10.0.19.238:9092/?topic=topicname"
