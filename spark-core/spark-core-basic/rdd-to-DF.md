from https://indatalabs.com/blog/convert-spark-rdd-to-dataframe-dataset

### // for implicit conversions from Spark RDD to Dataframe
```
import spark.implicits._

val dataFrame = rdd.toDF()
```
### covert rdd element to row, and define schema
```
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
val schema =  StructType(
    Seq(
      StructField(name = "manager_name", dataType = StringType, nullable = false),
      StructField(name = "client_name", dataType = StringType, nullable = false),
      StructField(name = "client_gender", dataType = StringType, nullable = false),
      StructField(name = "client_age", dataType = IntegerType, nullable = false),
      StructField(name = "response_time", dataType = DoubleType, nullable = false),
      StructField(name = "satisfaction_level", dataType = DoubleType, nullable = fals)
    )
  )

val data =
  rdd
    .mapPartitionsWithIndex((index, element) => if (index == 0) it.drop(1) else it) // skip header
    .map(_.split(",").to[List])
    .map(line => Row(line(0), line(1), line(2), line(3).toInt, line(4).toDouble, line(5).toDouble))

val dataFrame = spark.createDataFrame(data, schema)

```

From existing RDD by programmatically specifying the schema
```
def dfSchema(columnNames: List[String]): StructType =
  StructType(
    Seq(
      StructField(name = "name", dataType = StringType, nullable = false),
      StructField(name = "age", dataType = IntegerType, nullable = false)
    )
  )

def row(line: List[String]): Row = Row(line(0), line(1).toInt)

val rdd = sc.textFile('data/people_raw.txt')
val schema = dfSchema(Seq("name", "age"))
val data = rdd.map(_.split(",").to[List]).map(row)
val dataFrame = spark.createDataFrame(data, schema)
```

example data 
manager_name, client_name, client_gender, client_age, response_time, statisfaction_level
“Arjun Kumar”,”Rehan Nigam”,”male”,30,4.0,0.5
“Kabir Vish”,”Abhinav Neel”,”male”,28,12.0,0.1
“Arjun Kumar”,”Sam Mehta”,”male”,27,3.0,0.7
```
// create DataFrame from RDD (Programmatically Specifying the Schema)
val headerColumns = rdd.first().split(",").to[List] // extract headers [..] first
def dfSchema(columnNames: List[String]): StructType = {
  StructType(
    Seq(
      StructField(name = "manager_name", dataType = StringType, nullable = false),
      StructField(name = "client_name", dataType = StringType, nullable = false),
      StructField(name = "client_gender", dataType = StringType, nullable = false),
      StructField(name = "client_age", dataType = IntegerType, nullable = false),
      StructField(name = "response_time", dataType = DoubleType, nullable = false),
      StructField(name = "satisfaction_level", dataType = DoubleType, nullable = fals)
    )
  )
}
// create a data row for map transformation
def row(line: List[String]): Row = {
  Row(line(0), line(1), line(2), line(3).toInt, line(4).toDouble, line(5).toDouble)
}

// define a schema for the file

val schema = dfSchema(headerColumns)
val data =
  rdd
    .mapPartitionsWithIndex((index, element) => if (index == 0) it.drop(1) else it) // skip header
    .map(_.split(",").to[List])
    .map(row)

val dataFrame = spark.createDataFrame(data, schema)
```

```
data
  .groupBy($"manager_name")
  .agg(
    round(avg($"response_time"), 1).as("time"),
    round(avg($"satisfaction_level"), 1).as("satisfaction")
  )
  .orderBy($"satisfaction")
```

```
val viewName = s"summed"

val sql =
s"""
  SELECT manager_name,
  ROUND(SUM(response_time) / COUNT(response_time), 1) AS time,
  ROUND(SUM(satisfaction_level) / COUNT(satisfaction_level), 1) AS satisfaction
  FROM $viewName
  GROUP BY manager_name
  ORDER BY satisfaction

"""
// before run our SQL query we must create a temporary view
// for our DataFrame, by using following method
summed.createOrReplaceTempView(viewName)
spark.sql(sql)
```
