// give the user a nice default project!

name := "spark_example_App"
version := "0.1"
organization := "com.example.sparkproject"
scalaVersion := "2.12.10"
val sparkVersion = "2.4.0"

//%% means spark-core_scalaVersion, with single % you have to append _2.xx after spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.confluent" % "kafka-avro-serializer" % "3.1.1"
)

resolvers += "confluent" at "http://packages.confluent.io/maven/"
