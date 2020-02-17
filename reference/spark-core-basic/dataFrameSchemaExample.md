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
## clean csv file with header and string double quoted 
```
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




people.csv
"name","age","gender","timestamp","comment"
"jay",56,"Male",2014-08-27 11:29:31,NA
"helen",52,"Female",2014-08-27 11:29:31,NA


people_noheader.csv
"jay",56,"Male",2014-08-27 11:29:31,NA
"helen",52,"Female",2014-08-27 11:29:31,NA

people_raw.txt
jay,56,Male,2014-08-27 11:29:31,NA
helen,52,Female,2014-08-27 11:29:31,NA
