name := "Spark Kafka Project"
version := "1.0"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.0"


libraryDependencies ++= Seq (
"org.apache.spark" %% "spark-core" % sparkVersion,
"org.apache.spark" %% "spark-streaming" % sparkVersion,
"org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
)
