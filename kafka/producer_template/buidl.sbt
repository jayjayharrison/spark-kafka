name := "kafka_producer"

version := "1.0"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.3.1"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.11.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"
