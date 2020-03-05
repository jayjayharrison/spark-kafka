val rdd = sc.textFile("/home/jay/data/kaggle/credits.csv")


val rdd2 = rdd.map( _.replaceAll("\"\"","\'")).map( _.replaceAll("None","\'None\'"))

rdd2.repartition(1).saveAsTextFile("/home/jay/data/kaggle/rdd_dq/")

val df = spark.read
.schema("cast String, crew String, id Int, _corrupt_record String") 
.option("mode","PERMISSIVE") 
.option("badRecordsPath","/home/jay/data/kaggle")
.option("header","true")
.format("csv")
.load("/home/jay/data/kaggle/rdd_dq")

df.filter($"_corrupt_record".isNotNull).show

import org.apache.spark.sql.types._

val schema = ArrayType(new StructType()
.add("cast_id", StringType)
.add("character", StringType)
.add("credit_id", StringType)
.add("gender", IntegerType)
.add("id", IntegerType)
.add("name", StringType)
.add("order", IntegerType)
.add("profile_path", StringType)
)

val df2 = df.select(regexp_replace(regexp_replace($"cast", "(character':\\s+)\'", "$1\""),"\'(,\\s+'credit_id)","\"$1").as("cast"),$"crew",$"id",$"_corrupt_record")
.select(regexp_replace($"cast", "(name':\\s)\'(.{0,100})\'(,\\s+'order)", "$1\"$2\"$3").as("cast"),$"crew",$"id",$"_corrupt_record") 


val df_json = df2.select(from_json($"cast",schema).as("cast"),$"crew",$"id",$"_corrupt_record")
df_json.filter($"cast".isNull).show

//df2.select($"cast").filter($"id" === 675).write.format("csv").save("/home/jay/data/dfsave675")
df_json.printSchema

val df_exploded = df_json.withColumn("cast_2", explode($"cast"))  


val df4 = df_exp.select($"cast_2.cast_id",$"cast_2.name",$"cast_2.character", $"crew",$"id")
  
  
val df0_dq = df0.select(regexp_replace($"value","\"\"","\"").as("dq_value"))  

val schema0 = new StructType()
.add("cast", StringType)
.add("crew", StringType)
.add("id", IntegerType)

spark.createDataFrame(df0_dq.rdd,schema0)
  
  
val df = spark.read
.schema("cast String, crew String, id Int")
.option("mode","PERMISSIVE")
.option("header","true")
.format("csv")
.load("file:///home/sshuser/data/credits.csv")



val df2 = df.filter($"id".isNotNull)



val schema = ArrayType(new StructType()
.add("cast_id", StringType)
.add("character", StringType)
.add("credit_id", StringType)
.add("credit_id", StringType)
.add("gender", IntegerType)
.add("id", IntegerType)
.add("name", StringType)
.add("order", IntegerType)
.add("profile_path", StringType)
)

val df3 = df2.select(from_json($"cast",schema),$"crew",$"id")
df3.printSchema


df2.select($"cast").filter($"cast".like("%Max Goldman%")).write.save("file:///home/sshuser/data/cast")

