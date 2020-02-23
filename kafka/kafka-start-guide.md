### installation instruction 
```
wget http://apache.osuosl.org/kafka/2.4.0/kafka_2.12-2.4.0.tgz
tar -zxvf kafka_2.12-2.4.0.tgz
ls kafka_2.12-2.4.0/bin
```
### start zookeeper
```
kafka_2.12-2.4.0/bin/zookeeper-server-start.sh kafka_2.12-2.4.0/config/zookeeper.properties
# INFO binding to port 0.0.0.0/0.0.0.0:2181 (org.apache.zookeeper.server.NIOServerCnxnFactory)
# nohup kafka_2.12-2.4.0/bin/zookeeper-server-start.sh kafka_2.12-2.4.0/config/zookeeper.properties > nohup.out 2> nohup.err < /dev/null &
```
### after ZooKeeper started, create a new console for kafka server  or use nohup command to run process in backgroup
```
jps -l
kafka_2.12-2.4.0/bin/kafka-server-start.sh kafka_2.12-2.4.0/config/server.properties 
# nohup kafka_2.12-2.4.0/bin/kafka-server-start.sh kafka_2.12-2.4.0/config/server.properties > nohup.out 2> nohup.err < /dev/null &
# jobs -l # to list running nohup job in current shell session. OR ps -ef | grep "nohup "

```
### open a new console for kafka topic 
```
kafka_2.12-2.4.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
kafka_2.12-2.4.0/bin/kafka-topics.sh --list --zookeeper localhost:2181
```
### create producer and submit to topic
```
message=("hell" "where are you from" "im from iceland" "oh, nice, how cold it there" "no cold at all")
for x in {1..100}; do echo "Message $x : ${message[$(( ${RANDOM} % ${#message[@]} ))]}"; sleep 2 ; done | kafka_2.12-2.4.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

```
### create consumber
```
kafka_2.12-2.4.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```
