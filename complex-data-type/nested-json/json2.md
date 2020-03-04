https://docs.databricks.com/_static/notebooks/transform-complex-data-types-scala.html

```
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(sc.parallelize(Array(json)))
}
```
## Selecting from nested columns - Dots (".") can be used to access nested columns for structs and maps.
```
// Using a struct
val schema = new StructType().add("a", new StructType().add("b", IntegerType))
                          
val events = jsonToDataFrame("""
{
  "a": {
     "b": 1
  }
}
""", schema)
display(events.select("a.b"))
```
### // Using a map
```
val schema = new StructType().add("a", MapType(StringType, IntegerType))
                          
val events = jsonToDataFrame("""
{
  "a": {
     "b": 1
  }
}
""", schema)

display(events.select("a.b"))
```
```
val events = jsonToDataFrame("""
{
  "a": {
     "b": 1,
     "c": 2
  }
}
""")

display(events.select("a.*"))
```
