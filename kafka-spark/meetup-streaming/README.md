
### push meetup streaming to kafka
```
curl http://stream.meetup.com/2/rsvps | kafka_2.12-2.5.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic meetup
```
### 
```
spark-shell \
  --packages com.typesafe:config:1.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0
```
