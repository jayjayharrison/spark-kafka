```
1,Jay,25,true,87|332|123
2,Amy,22,true,121|12|1221|123
3,Ali,26,false,123|123

import scala.collection.mutable.ArrayBuffer

case class Person(id:Int, name:String, age:Int, isNew:Boolean, classCode:Array[Int] )


def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val id:Int = fields(0).toInt
    val name:String = fields(1)
    val age:Int = fields(2).toInt
    val isNew:Boolean = fields(3).toBoolean
    
    val classCodeListStr: Array[String] = fields(4).split('|')
    
    var classCodeBuffer:ArrayBuffer[Int] = ArrayBuffer()
    
    for ( classCd <- 0 to (classCodeListStr.length -1 )) {
      classCodeBuffer += classCodeListStr(classCd).trim.toInt
    }
    
    val person:Person = Person(id,name, age, isNew, classCodeBuffer.toArray)
    return person
}

val rdd = sc.textFile("dataset/demo.csv")

val people = rdd.map(mapper)

val peopleds = people.toDS


scala> peopleds.show()
+---+----+---+-----+--------------------+
| id|name|age|isNew|           classCode|
+---+----+---+-----+--------------------+
|  1| Jay| 25| true|      [87, 332, 123]|
|  2| Amy| 22| true|[121, 12, 1221, 123]|
|  3| Ali| 26|false|          [123, 123]|
+---+----+---+-----+--------------------+


```


```
val arrayStructureData = Seq(
    Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
    Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
    Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F")
  )

  val arrayStructureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df.printSchema()
  df.show()

root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- languages: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- state: string (nullable = true)
 |-- gender: string (nullable = true)

+--------------------+------------------+-----+------+
|                name|         languages|state|gender|
+--------------------+------------------+-----+------+
|    [James, , Smith]|[Java, Scala, C++]|   OH|     M|
|      [Anna, Rose, ]|[Spark, Java, C++]|   NY|     F|
| [Julia, , Williams]|      [CSharp, VB]|   OH|     F|
|[Maria, Anne, Jones]|      [CSharp, VB]|   NY|     M|
|  [Jen, Mary, Brown]|      [CSharp, VB]|   NY|     M|
|[Mike, Mary, Will...|      [Python, VB]|   OH|     M|
+--------------------+------------------+-----+------+

```


