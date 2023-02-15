## Concepts
1. Kafka topic can be divided into multiple partitions
2. Each partition stored different record (thats what provides the scalablity/parallelism)
3. In a consumer group, One consumer can read record from multiple partitions, but one partitions can not be read by more than one consumer.


/usr/hdp/current/kafka-broker/

## Kafka Quick Start
### Installation Instruction 
```
wget http://apache.osuosl.org/kafka/2.4.0/kafka_2.12-2.4.0.tgz
tar -zxvf kafka_2.12-2.4.0.tgz
ls kafka_2.12-2.4.0/bin
cd kafka_2.12-2.4.0
```
### Start Zookeeper Server
```
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
# check kafka_2.12-2.4.0/logs/server.log
# nohup job much end with a '&' to tell it to run in background
# nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ~/nohup.out 2> ~/nohup.err < /dev/null &
# send standard out to home/nohup.out and send standard error to home/nohup.err
```
### Start Kafka Server 
```
bin/kafka-server-start.sh -daemon config/server.properties 
# nohup bin/kafka-server-start.sh config/server.properties > ~/kafka_nohup.out 2> ~/kafka_nohup.err < /dev/null &
# jobs -l # to list running nohup job in current shell session. OR ps -ef | grep "nohup "
# jps -l
```
### Create Kafka Topic 
```
kafka_2.12-2.4.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
kafka_2.12-2.4.0/bin/kafka-topics.sh --list --zookeeper localhost:2181
```
### Start Producer 
```
curl http://stream.meetup.com/2/rsvps | kafka-console-producer.sh --broker-list localhost:9092 --topic test

message=("hell" "where are you from" "im from iceland" "oh, nice, how cold it there" "no cold at all")
for x in {1..100}; do echo "Message $x : ${message[$(( ${RANDOM} % ${#message[@]} ))]}"; sleep 2 ; done | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```
### Start Consumber
```
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

### Cluster Env
```
/opt/kafka/bin/kafka-topics.sh \
  --create \
  --zookeeper kdbroker01.itversity.com:2181,kdbroker02.itversity.com:2181,kdbroker03.itversity.com:2181 \
  --replication-factor 1 \
  --partitions 1 \
  --topic test

/opt/kafka/bin/kafka-console-producer.sh \
  --broker-list kdbroker04.itversity.com:9092,kdbroker05.itversity.com:9092 \
  --topic test

/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kdbroker04.itversity.com:9092,kdbroker05.itversity.com:9092 \
  --topic test \
  --from-beginning
```
