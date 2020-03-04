###  get_json_object

```
val data = Seq (
 (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
 (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""))
 .toDF("id", "json_string")
 
val jsDF = data.select($"id", get_json_object($"json_string", "$.device_type").alias("device_type"),
                                          get_json_object($"json_string", "$.ip").alias("ip"),
                                         get_json_object($"json_string", "$.cca3").alias("cca3"))

scala> jsDF.show()
+---+-------------+-------------+----+
| id|  device_type|           ip|cca3|
+---+-------------+-------------+----+
|  0|  sensor-ipad| 68.161.225.1| USA|
|  1|sensor-igauge|213.161.254.1| NOR|
+---+-------------+-------------+----+

```

### Covert column to sturct type
```
import org.apache.spark.sql.types._                         // include the Spark Types to define our schema
import org.apache.spark.sql.functions._                     // include the Spark helper functions
val jsonSchema = new StructType()
        .add("battery_level", LongType)
        .add("c02_level", LongType)
        .add("cca3",StringType)
        .add("cn", StringType)
        .add("device_id", LongType)
        .add("device_type", StringType)
        .add("signal", LongType)
        .add("ip", StringType)
        .add("temp", LongType)
        .add("timestamp", TimestampType)
        
case class DeviceData (id: Int, json_string: String)

val dataDS = data.as[DeviceData]
val newdf = data.select(from_json($"json_string", jsonSchema) as "devices") // newdf.printSchema,  column is now struct type
.select($"devices.*")        // select * from device column
.filter($"devices.temp" > 10 and $"devices.signal" > 15)

```
https://docs.databricks.com/spark/latest/dataframes-datasets/complex-nested-data.html

### split a column to array, and explode to row
```
scala> val df = jsDF.withColumn("arr",split($"ip","\\."))
+---+-------------+-------------+----+------------------+
| id|  device_type|           ip|cca3|               arr|
+---+-------------+-------------+----+------------------+
|  0|  sensor-ipad| 68.161.225.1| USA| [68, 161, 225, 1]|
|  1|sensor-igauge|213.161.254.1| NOR|[213, 161, 254, 1]|
+---+-------------+-------------+----+------------------+

scala> df.withColumn("col2",explode($"arr")).show()
+---+-------------+-------------+----+------------------+----+
| id|  device_type|           ip|cca3|               arr|col2|
+---+-------------+-------------+----+------------------+----+
|  0|  sensor-ipad| 68.161.225.1| USA| [68, 161, 225, 1]|  68|
|  0|  sensor-ipad| 68.161.225.1| USA| [68, 161, 225, 1]| 161|
|  0|  sensor-ipad| 68.161.225.1| USA| [68, 161, 225, 1]| 225|
|  0|  sensor-ipad| 68.161.225.1| USA| [68, 161, 225, 1]|   1|
|  1|sensor-igauge|213.161.254.1| NOR|[213, 161, 254, 1]| 213|
|  1|sensor-igauge|213.161.254.1| NOR|[213, 161, 254, 1]| 161|
|  1|sensor-igauge|213.161.254.1| NOR|[213, 161, 254, 1]| 254|
|  1|sensor-igauge|213.161.254.1| NOR|[213, 161, 254, 1]|   1|
+---+-------------+-------------+----+------------------+----+

```
