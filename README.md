# spark-core-quick-start

## create spark session 
```
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder
.appName("SparkSessionExample") 
.master("local[4]") 
.config("spark.sql.warehouse.dir", "target/spark-warehouse")
.enableHiveSupport() //enables access to Hive metastore, Hive serdes, and Hive udfs.
.getOrCreate
```

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
val df = spark.read.option("delimiter", "\t").csv("data/people.csv")
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

df.write
.format("csv")
.option("header","true")
.option("nullValue","NA")
.made("overwrite")
.save('')

```
## Writer parquet with partition
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

```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Create Spark Session
val spark = SparkSession.builder.master("local").appName("Window Function").getOrCreate()
import sparkSession.implicits._

// Create Sample Dataframe
val empDF = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
	
empDF.select($"*", rank().over(Window.partitionBy($"deptno").orderBy($"sal".desc)) as "rank").filter($"rank" < 2).show
+-----+-----+---------+----+---------+----+----+------+----+
|empno|ename|      job| mgr| hiredate| sal|comm|deptno|rank|
+-----+-----+---------+----+---------+----+----+------+----+
| 7788|SCOTT|  ANALYST|7566|19-Apr-87|3000|   0|    20|   1|
| 7839| KING|PRESIDENT|   0|17-Nov-81|5000|   0|    10|   1|
| 7698|BLAKE|  MANAGER|7839| 1-May-81|2850|   0|    30|   1|
+-----+-----+---------+----+---------+----+----+------+----+

empDF.groupBy($"deptno",$"job").agg(min($"sal"),max($"sal")).show()
+------+---------+--------+--------+
|deptno|      job|min(sal)|max(sal)|
+------+---------+--------+--------+
|    20|  ANALYST|    3000|    3000|
|    20|  MANAGER|    2975|    2975|
|    30|  MANAGER|    2850|    2850|
|    30| SALESMAN|    1250|    1600|
|    20|    CLERK|    1100|    1100|
|    10|PRESIDENT|    5000|    5000|
|    10|    CLERK|     800|     800|
|    10|  MANAGER|    2450|    2450|
+------+---------+--------+--------+



empDF.groupBy($"deptno",$"job").count().show()
+------+---------+-----+
|deptno|      job|count|
+------+---------+-----+
|    20|  ANALYST|    1|
|    20|  MANAGER|    1|
|    30|  MANAGER|    1|
|    30| SALESMAN|    4|
|    10|PRESIDENT|    1|
|    20|    CLERK|    1|
|    10|    CLERK|    1|
|    10|  MANAGER|    1|
+------+---------+-----+
 
    
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
```
scala> :help
...
:shortcuts     print JLine shortcuts
...

scala> :short
Keybinding mapping for JLine. The format is:
[key code]: [logical operation]
CTRL-B: move to the previous character
CTRL-G: move to the previous word
CTRL-F: move to the next character
CTRL-A: move to the beginning of the line
CTRL-D: close out the input stream
CTRL-E: move the cursor to the end of the line
BACKSPACE, CTRL-H: delete the previous character
8 is the ASCII code for backspace and therefor
deleting the previous character
TAB, CTRL-I: signal that console completion should be attempted
CTRL-J, CTRL-M: newline
CTRL-K: erase the current line
ENTER: newline
CTRL-L: clear screen
CTRL-N: scroll to the next element in the history buffer
CTRL-P: scroll to the previous element in the history buffer
CTRL-R: redraw the current line
CTRL-U: delete all the characters before the cursor position
CTRL-V: paste the contents of the clipboard (useful for Windows terminal)
CTRL-W: delete the word directly before the cursor
DELETE, CTRL-?: delete the previous character
127 is the ASCII code for delete
```

