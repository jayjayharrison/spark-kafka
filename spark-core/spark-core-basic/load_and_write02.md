Copy from https://www.learningjournal.guru/courses/spark/spark-foundation-training/file-based-data-sources/
1. Read CSV -> Write Parquet
2. Read Parquet -> Write JSON
3. Read JSON -> Write ORC
4. Read ORC -> Write XML
5. Read XML -> Write AVRO
6. Read AVRO -> Write CSV

### 1.Read CSV -> Write Parquet
```
//Read CSV into Data Frame
val df = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("nullValue", "NA")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
  .option("mode", "failfast")
  .load("/home/prashant/spark-data/mental-health-in-tech-survey/survey.csv")

//Write Data Frame to Parquet
df.write
  .format("parquet")
  .mode("overwrite")
  .save("/home/prashant/spark-data/mental-health-in-tech-survey/parquet-data/")
```
### 2. Read Parquet -> Write JSON
```
//Read Parquet into Data Frame
val df = spark.read
  .format("parquet")
  .option("mode", "failfast")
  .load("/home/prashant/spark-data/mental-health-in-tech-survey/parquet-data/")

//Write Data Frame to JSON
df.write
  .format("json")
  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
  .mode("overwrite")
  .save("/home/prashant/spark-data/mental-health-in-tech-survey/json-data/")
```
### 3. Read JSON -> Write ORC
```
//Read JSON into Data Frame
val df = spark.read
  .format("json")
  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
  .option("mode", "failfast")
  .load("/home/prashant/spark-data/mental-health-in-tech-survey/json-data/")

//Write Data Frame to ORC
df.write
  .format("orc")
  .mode("overwrite")
  .save("/home/prashant/spark-data/mental-health-in-tech-survey/orc-data/")
```
### 4. Read ORC -> Write XML

#### Dependencies: spark-shell --packages com.databricks:spark-xml_2.11:0.4.1,com.databricks:spark-avro_2.11:4.0.0 

```
//Read ORC into Data Frame
val df = spark.read
  .format("orc")
  .option("mode", "failfast")
  .load("/home/prashant/spark-data/mental-health-in-tech-survey/orc-data/")

//Write Data Frame to XML
df.write
  .format("com.databricks.spark.xml")
  .option("rootTag", "survey")
  .option("rowTag", "survey-row")
  .mode("overwrite")
  .save("/home/prashant/spark-data/mental-health-in-tech-survey/xml-data/")

```

```
//Read XML into Data Frame
val df = spark.read
  .format("com.databricks.spark.xml")
  .option("rowTag", "survey-row")
  .option("mode", "failfast")
  .load("/home/prashant/spark-data/mental-health-in-tech-survey/xml-data/")

//Write Data Frame to AVRO
df.write
  .format("com.databricks.spark.avro")
  .mode("overwrite")
  .save("/home/prashant/spark-data/mental-health-in-tech-survey/avro-data/")
```


