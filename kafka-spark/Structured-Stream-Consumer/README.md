```
sbt "run dev"

sbt package

  spark-submit \
  --class com.example.sparkstream.stream_processing_app \
  --name stream_processing_app \
  --packages com.typesafe:config:1.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  --master local \
  ./target/scala-2.12/sparkstream_2.12-0.0.1.jar dev
  
 ```
