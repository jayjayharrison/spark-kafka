  val Data = Seq(
    Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
    Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
    Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
  )

  val Schema = 
  new StructType()
  .add("name",StringType)
  .add("subjects",ArrayType(
                    ArrayType(StringType)
                            )
      )

  val df = spark.createDataFrame(
     spark.sparkContext.parallelize(Data), Schema)
  
import spark.implicits._
val df2 = df.select($"name",explode($"subjects"))
  
df.select($"name",flatten($"subjects")).show(false)
