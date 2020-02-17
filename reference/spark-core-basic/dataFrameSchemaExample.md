https://www.learningjournal.guru/courses/spark/spark-foundation-training/spark-sql-over-dataframe/'
```
import org.apache.spark.sql.types._
val surveySchema = StructType(
  Array(
    StructField("name", StringType, true),
    StructField("age", LongType, true),
    StructField("gender", StringType, true),
    StructField("timestamp", TimestampType, true),
    StructField("comment", StringType, true)
  )
)
```
//You can load the data using above schema
:paste
val df = spark.read
  .format("csv")
  .schema(surveySchema)
  .option("header", "true")
  .option("nullValue", "NA")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
  .option("mode", "failfast")
  .load("data/people.csv")
  
  
  "name","age","gender","timestamp","comment"
  "jay",56,"Male",2014-08-27 11:29:31,NA
  "helen",52,"Female",2014-08-27 11:29:31,NA
  
  "name","age","gender","timestamp","comment"
  "jay",56,"Male",2014-08-27 11:29:31,NA
  "helen",52,"Female",2014-08-27 11:29:31,NA


jay,56,Male,2014-08-27 11:29:31,NA
helen,52,Female,2014-08-27 11:29:31,NA
