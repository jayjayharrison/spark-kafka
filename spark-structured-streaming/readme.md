https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#deploying
https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```
```
spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0
```

```
val kafka_stream = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "test")
  .option("startingOffsets", "latest")
  .load()

//read stream return key value pair, key is topic name and value is the message

val message = kafka_stream.selectExpr("CAST(value AS STRING)").as[String]
val words = message.flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()

val query = wordCounts.writeStream
  .outputMode("complete") //complete, all, append
  .format("console")
  .start()

query.awaitTermination()

```

```
val df = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "test")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .load()
  
val ds = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").as[(String, String)]
ds.show()
  
```
