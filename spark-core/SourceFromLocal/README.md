```
import scala.io.Source
import org.apache.spark.sql._
import spark.implicits._

val lines = Source.fromFile("data").getLines().toList()

val rdd = sc.makeRDD(lines)

val rdd_arr = rdd.map( s=> s.split(",") )

val rdd_kv = rdd_arr.map( arr => (arr(0), arr(1)))


```
