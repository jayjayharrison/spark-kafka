# spark-core-basic
##data/people.json      {"name":"jay","age",39},{"name":"Linsey","age",22}
```
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

##RDD
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
