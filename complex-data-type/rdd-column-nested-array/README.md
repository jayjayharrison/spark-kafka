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
