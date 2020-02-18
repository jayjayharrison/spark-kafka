package application using sbt 

below is the file structure of sbt project 
```
project
  -build.sbt
  -src
    -main
      -scala
        -test.scala
```

in build.sbt manage your dependency 
```
name := "spark Test App"
version := "0.1"
organization := "guru.learningjournal"
scalaVersion := "2.11.8"
val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.confluent" % "kafka-avro-serializer" % "3.1.1"
)

resolvers += "confluent" at "http://packages.confluent.io/maven/"
```

test.scala
```
package guru.learningjournal.examples

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SparkTestApp {
 def main(args: Array[String]) = {
  val spark = SparkSession.builder()
  .appName("SparkTestApp")
  .enableHiveSupport()
  .getOrCreate()

  spark.sql("""select * from hivesampletable limit 5""")
   .write
   .format("csv")
   .save("file:///home/sshuser/out/")
   spark.stop()
  }
 }

```

Run 'sbt package' on project root directory, then jar file will be created at target folder

```
spark-submit --master local --class guru.learningjournal.examples.SparkTestApp target/scala-2.11/spark-test-app_2.11-0.1.jar InputIfAny
```

