```
val df0 = spark.read
.schema("value String")
.option("mode","PERMISSIVE")
.option("header","true")
.format("csv")
.load("file:///home/sshuser/data/credits.csv")
  
val df0_dq = df0.select(regexp_replace($"value","\"\"","\"").as("dq_value"))  

val schema0 = new StructType()
.add("cast", StringType)
.add("crew", StringType)
.add("id", IntegerType)

spark.createDataFrame(df0_dq.rdd,schema0)
  
  
val df = spark.read\
.schema("cast String, crew String, id Int")\
.option("mode","PERMISSIVE")\
.option("header","true")\
.format("csv")\
.load("file:///home/sshuser/data/credits.csv")\



val df2 = df.filter($"id".isNotNull)

import org.apache.spark.sql.types._

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

df2.select($"cast").filter($"cast".like("%Max Goldman%")).write.save("file:///home/sshuser/data/cast")
```
