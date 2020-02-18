name := "fat-jars"

version := "1.0"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion  % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided"

mainClass := Some("com.queirozf.StreamingExample")
