## Kafka-Spark-Dstream Quick start
check your scala and spark version for kafka package
```
spark-shell --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.0
```

run a :paste command 
```

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

val ssc = new StreamingContext(sc, Seconds(2))

val topics = Array("message")
val brokers = "localhost:9092"
val groupId = "grp_id"

val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])


val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

// Get the lines, split them into words, count the words and print
val lines = messages.map(_.value)
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
wordCounts.print()

// Start the computation
ssc.start()
ssc.awaitTermination()

```
### create kafka topic test and start producer 
```
kafka_2.12-2.4.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
message=("hell" "where are you from" "im from iceland" "oh, nice, how cold it there" "no cold at all" "what is this" "trump trump")
for x in {1..100}; do echo "Message $x : ${message[$(( ${RANDOM} % ${#message[@]} ))]}"; sleep 0.5 ; done | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

```
