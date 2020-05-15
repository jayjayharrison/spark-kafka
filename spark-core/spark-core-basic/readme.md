# Data-Frame-quick-start

## dataFrame exmaple with window function
```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Create Spark Session
val spark = SparkSession.builder.master("local").appName("Window Function").getOrCreate()
import sparkSession.implicits._
```
### Create Sample Dataframe
```
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
	
```
### return top salary maker from each department 
```
empDF.select($"*", rank().over(Window.partitionBy($"deptno").orderBy($"sal".desc) ) as "rank").filter($"rank" < 2).show
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

empDF.groupBy($"job").agg(min($"sal"),max($"sal"),count($"job").alias("emp-cnt")).where(column("emp-cnt") >1 ).show()
+--------+--------+--------+-------+
|     job|min(sal)|max(sal)|emp-cnt|
+--------+--------+--------+-------+
|SALESMAN|    1250|    1600|      4|
|   CLERK|     800|    1100|      2|
| MANAGER|    2450|    2975|      3|
+--------+--------+--------+-------+

empDF.groupBy($"deptno",$"job").count().show()  // or empDF.select($"deptno",$"job").groupBy($"deptno",$"job").count()
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

import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.col

empDF.select(empDF.columns.map(c => countDistinct(col(c)).alias(c)): _*).show()
+-----+-----+---+---+--------+---+----+------+
|empno|ename|job|mgr|hiredate|sal|comm|deptno|
+-----+-----+---+---+--------+---+----+------+
|   11|   11|  5|  6|      11| 10|   5|     3|
+-----+-----+---+---+--------+---+----+------+

empDF.select($"deptno").groupBy($"deptno").agg(countDistinct($"deptno").alias("no_of_emp")).show()
|deptno|no_of_emp|
+------+---+
|    30|  1|
|    10|  1|
|    20|  1|
+------+---+


```
## // window function - find highest 3 salary in each dept 
```
val partitionWindow = Window.partitionBy($"dept").orderBy($"salary".desc)
val rankTest = rank().over(partitionWindow)
employee.select($"*", rankTest as "rank").filter($"rank" < 4)show

//OR
empDF.select($"*", rank().over(Window.partitionBy($"deptno").orderBy($"sal".desc)) as "rank").filter($"rank" < 2)show
//withColumn
empDF.withColumn("rank", rank() over Window.partitionBy("deptno").orderBy($"sal".desc)).filter($"rank" === 1).show()
```
## //find min salary, with rowFrame,   unboundedfllowwing represent last row on a desc ordering  
 default window frame is range between unbounded preceding and current row meaning top first row to current row, then in desc ordering, current row is always the last
```
empDF.select($"*", last($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc).rowsBetween(Window.currentRow, Window.unboundedFollowing)) as "rank").show
```
## count how many people have salavery less or equal to current row salary. 
##### OVER order by salary, have default frame from top row( lowest salary) to current salary.
##### Over order by salary DESC, have same window frame, but now top row is highest salary, so it will return how many people have salary greater or equal to you

```
sql("SELECT salary, count(*) OVER (ORDER BY salary) AS cnt FROM t_employee order by salary").show
```


## RDD


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
