# spark-core-basic
## DataFrame Load Json and Transformation

```
data/people.json      {"name":"jay","age",39},{"name":"Linsey","age",22}
val df = spark.read.json("data/people.json")
df.filter("age > 30").select("name", "age").show()
//for a list of Methods inherited from class org.apache.spark.sql.Column, https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/sql/ColumnName.html

df.select($"name", $"age" + 1,$"name".isNull,$"name".substr(1,1).alias("Initial"),when($"age">30,"mid age").when($"age"<=30,"younge").otherwise("na").alias("age group")).show()

+------+---------+--------------+-------+---------+
|  name|(age + 1)|(name IS NULL)|Initial|age group|
+------+---------+--------------+-------+---------+
|   jay|       40|         false|      j|  mid age|
|Linsey|       23|         false|      L|   younge|
+------+---------+--------------+-------+---------+

df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people where age > 21").show()
```
// aggregation
```
val df_cnt = df.groupby("age").count()
```
// window function - find eldest person in a group 
```
val df2 = df.withColumn("eldest_person_in_a_group", max("age") over Window.partitionBy("some_group"))
    .filter($"age" === $"eldest_person_in_a_group")

val df2 = df.withColumn("eldest_person_in_a_group", rank() over Window.partitionBy("some_group").orderBy("age") as rnk)
    .filter($"rnk"=== 1)
```

// window function - find highest 3 salary in each dept 
```
val partitionWindow = Window.partitionBy($"dept").orderBy($"salary".desc)
val rankTest = rank().over(partitionWindow)
employee.select($"*", rankTest as "rank").filter($"rank" < 4)show

//OR

empDF.select($"*", rank().over(Window.partitionBy($"deptno").orderBy($"sal".desc)) as "rank").filter($"rank" < 2)show

//withColumn
empDF.withColumn("rank", rank() over Window.partitionBy("deptno").orderBy($"sal".desc)).filter($"rank" === 1).show()
```
//find min salary, with rowFrame,   unboundedfllowwing represent last row on a desc ordering  
 default window frame is range between unbounded preceding and current row, then in desc ordering, current row is always the last
```
empDF.select($"*", last($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc).rowsBetween(Window.currentRow, Window.unboundedFollowing)) as "rank").show
```
//join
```
people.filter("age > 30")
     .join(department, people("deptId") === department("id"))
     .groupBy(department("name"), "gender")
     .agg(avg(people("salary")), max(people("age")))
```
## Load CSV with options
```
val df = spark.read.csv("data/people.csv")
or 
val df = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true") // inferSchema will allow spark to automatecally map the DDL. It is recommanded to define your own schema when loading untyped file such as csv/json. See spark-df-schema
  .option("nullValue", "NA")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm?:ss")
  .option("mode", "failfast")
  .option("path", "survey.csv")
  .load()
```
## Writer with partition
```
df.write
  .format("parquet")
  .partitionBy("column_name")
  .bucketBy("column_name")
  .sortBy("column_name")
  .mode("overwrite") //append|overwrite|errorIfExists|ignore
  .save("path")
```

## RDD
```
sudo find / > flist.txt # list all directory to flist.txt 
//hadoop fs -copyFromLocal flist.txt /user/jay
wc -l flist.txt
cat flist.txt | head -n10
spark-shell
val flistRDD = sc.textFile("flist.txt", 5) // split into 5 partition
flistRDD.count()
flistRDD.foreachPartition(p =>println("No of Items in partition-" + p.count(y=>true)))
flistRDD.getNumPartitions
val listRDD = flistRDD.map(x=> x.split("/")) // convertinto an array RDD
val kvRDD= listRDD.map(a => (a(0),1)) # convert the list into a tuple/key value pair. (first element, 1) ir. (bin,1)
val fcountRDD = kvRDD.reduceByKey( (x,y)=> x+y ) 
fcountRDD.collect() # return result RDD to driver
```



sudo find / > flist.txt                # list all directory to flist.txt
hadoop fs -ls /user

hadoop fs -copyFromLocal flist.txt /user/jay

hadoop fs -ls /user/jay

spark-shell

val flistRDD = sc.textFile("/user/jay/filist.txt", 5)   # split into 5 partition

val listRDD = flistRDD.map(x=> x.split("/"))            # convertinto an array RDD

val kvRDD= listRDD.map(a => (a(0),1))                   # convert the list into a tuple/key value pair. (first element, 1) ir. (bin,1)

val fcountRDD = kwRDD.reduceByKey( (x,y)=> x+y )        # 

fcountRDD.collect()                                     # return result RDD to driver 

