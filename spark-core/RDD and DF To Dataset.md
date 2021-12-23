### From RDD to DS,  map RDD to a case class 

```
case class Person(ID:Int, name:String, age:Int, numFriends:Int)
    
val people = rdd.map(mapper) // return RDD of Person   rdd.map( Person(_.split(',')(0).toInt, _.split(',')(1).toString .... ))

import spark.implicits._
val schemaPeople = people.toDS


def mapper(line:String): Person = {
    val fields = line.split(',')  
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person }
```

### From Data Frame to DS

```
import org.apache.spark.sql.SparkSession

case class ratings(userId:Integer, movieId:Integer, rating:Double, timestamp:Integer) //muct match the df schema 

// From DataFrame to DataSet
val df = spark.read.option("header","true").option("inferSchema","true").csv("data/kaggle/ratings_small.csv")
val ds = df.as[ratings]


// From RDD to DataSet
val rdd = sc.textFile("data/kaggle/ratings_small.csv")
val data =rdd.mapPartitionsWithIndex((index, element) => if (index == 0) element.drop(1) else element)
val ds   =data.map(_.split(","))
              .map(elem => ratings(elem(0).trim.toInt,elem(1).trim.toInt, elem(2).trim.toDouble,elem(3).trim.toInt))
              .toDF().as[ratings]
```
