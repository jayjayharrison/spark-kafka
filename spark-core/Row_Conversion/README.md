
val order = Seq((1, "0012P"), (1, "448xx"), (2, "553"),(2, "5788"),(3,"4aa1")).toDF("cust_id", "prod_id")

import spark.implicits._
val CustomerOrderCount = order.groupBy($"cust_id", $"prod_id").count

CustomerOrderCount.printSchema

root
 |-- cust_id: integer (nullable = false)
 |-- prod_id: integer (nullable = false)
 |-- count: long (nullable = false)
There are a few ways to access Row values and keep expected types:

Pattern matching

case class data(cust_id: Int, prod_id: String, count: Int)

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

CustomerOrderCount.map{
  case GenericRowWithSchema(a: Int, b: String, c: Int) => data(a, b, c) 
  } 
  
Typed get* methods like getInt, getLong:

CustomerOrderCount.map(
  r => data(r.getInt(0), r.getString(1), r.getInt(2))
).show

getAs method which can use both names and indices:

transactions_with_counts.map(r => Rating(
  r.getAs[Int]("user_id"), r.getAs[Int]("category_id"), r.getAs[Long](2)
))
It can be used to properly extract user defined types, including mllib.linalg.Vector. Obviously accessing by name requires a schema.

Converting to statically typed Dataset (Spark 1.6+ / 2.0+):

transactions_with_counts.as[(Int, Int, Long)]
