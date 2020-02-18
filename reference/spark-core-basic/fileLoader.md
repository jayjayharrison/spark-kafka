
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
Or define schema using a DDL like string
val df= spark.read.schema("a STRING, b Int, c STRING, D TimeStamp").format("csv").load("data/people_raw.txt")

or with csv api
val df = spark.read.csv("data/people_noheader.csv")
```
## clean csv file with header and string double quoted 
```
people.csv
"name","age","gender","timestamp","comment"
"jay",56,"Male",2014-08-27 11:29:31,NA
"helen",52,"Female",2014-08-27 11:29:31,NA

:paste
val df = spark.read
  .format("csv")
  .schema(surveySchema)
  .option("header", "true")
  .option("nullValue", "NA")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
  .option("mode", "failfast")
  .load("data/people.csv")

df.show()
+-----+---+------+-------------------+-------+
| name|age|gender|          timestamp|comment|
+-----+---+------+-------------------+-------+
|  jay| 56|  Male|2014-08-27 11:29:31|   null|
|helen| 52|Female|2014-08-27 11:29:31|   null|
+-----+---+------+-------------------+-------+

```
## No header csv file and string double quoted 
```
"jay",56,"Male",2014-08-27 11:29:31,NA
"helen",52,"Female",2014-08-27 11:29:31,NA

:paste
val df = spark.read
  .format("csv")
  .schema(surveySchema)
  .option("header", "false")
  .option("nullValue", "NA")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
  .option("mode", "failfast")
  .load("data/people_noheader.csv")

df.show()
+-----+---+------+-------------------+-------+
| name|age|gender|          timestamp|comment|
+-----+---+------+-------------------+-------+
|  jay| 56|  Male|2014-08-27 11:29:31|   null|
|helen| 52|Female|2014-08-27 11:29:31|   null|
+-----+---+------+-------------------+-------+

val df_inferSchema = spark.read
  .format("csv")
  .option("header", "false")
  .option("inferSchema", "true") 
  .option("nullValue", "NA")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm?:ss")
  .option("mode", "failfast")
  .option("path", "data/people_noheader.csv")
  .load()
scala> df_inferSchema.show()
+-----+---+------+-------------------+----+
|  _c0|_c1|   _c2|                _c3| _c4|
+-----+---+------+-------------------+----+
|  jay| 56|  Male|2014-08-27 11:29:31|null|
|helen| 52|Female|2014-08-27 11:29:31|null|
+-----+---+------+-------------------+----+

```

## raw text file
```
jay,56,Male,2014-08-27 11:29:31,NA
helen,52,Female,2014-08-27 11:29:31,NA

val df_raw = spark.read
  .format("csv")
  .option("header", "false")
  .option("inferSchema", "true") 
  .option("nullValue", "NA")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm?:ss")
  .option("mode", "failfast")
  .option("path", "data/people_raw.txt")
  .load()
  
scala> df_raw.show()
+-----+---+------+-------------------+----+
|  _c0|_c1|   _c2|                _c3| _c4|
+-----+---+------+-------------------+----+
|  jay| 56|  Male|2014-08-27 11:29:31|null|
|helen| 52|Female|2014-08-27 11:29:31|null|
+-----+---+------+-------------------+----+
```

## raw text file with pipe/other delimiter .option("delimiter", "|")
```
val df_raw_pipe = spark.read
  .format("csv")
  .option("delimiter", "|") 
  .option("header", "false")
  .option("inferSchema", "true") 
  .option("nullValue", "NA")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm?:ss")
  .option("mode", "failfast")
  .option("path", "data/people_pipe.txt")
  .load()
+-----+---+------+-------------------+----+
|  _c0|_c1|   _c2|                _c3| _c4|
+-----+---+------+-------------------+----+
|  jay| 56|  Male|2014-08-27 11:29:31|null|
|helen| 52|Female|2014-08-27 11:29:31|null|
+-----+---+------+-------------------+----+

```



