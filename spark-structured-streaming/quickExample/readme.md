```
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("StructuredNetworkWordCount")
  .getOrCreate()
  
import spark.implicits._

/ Create DataFrame representing the stream of input lines from connection to localhost:9999
val lines = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()

// Split the lines into words
val words = lines.as[String].flatMap(_.split(" "))

// Generate running word count
val wordCounts = words.groupBy("value").count()

// Start running the query that prints the running counts to the console
val query = wordCounts.writeStream
  .outputMode("complete")
  .format("console")
  .start()

query.awaitTermination()

```
### run a netcat server on port 9999 
```
nc -lk 9999
hello
can you hear me?
yes?
who are you

```
```
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
+-----+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|  you|    1|
|  can|    1|
| hear|    1|
|hello|    1|
|  me?|    1|
+-----+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|  you|    1|
|  can|    1|
| hear|    1|
|hello|    1|
|  me?|    1|
| yes?|    1|
+-----+-----+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|  who|    1|
|  you|    2|
|  can|    1|
| hear|    1|
|hello|    1|
|  me?|    1|
|  are|    1|
| yes?|    1|
+-----+-----+

```
