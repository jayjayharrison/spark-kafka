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


//Create a local function
  val parseGender = (s: String) => {
   if (List("cis female", "f", "female", "woman", "femake", "female ",
     "cis-female/femme", "female (cis)", "femail").contains(s.toLowerCase))
    "Female"
   else if (List("male", "m", "male-ish", "maile", "mal", "male (cis)",
     "make", "male ", "man", "msle", "mail", "malr", "cis man", "cis male").contains(s.toLowerCase))
    "Male"
   else
    "Transgender"
  }

  //Register the function as UDF
  spark.udf.register("PGENDER", parseGender)


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

