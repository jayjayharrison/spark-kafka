## Tips

## 1) execute external system commands in Scala
```
import sys.process._
Use the .! method to execute the command and get its exit status.
Use the .!! method to execute the command and get its output.
Use the .lines method to execute the command in the background and get its result as a Stream.

"ls -al".!
"hdfs -dfs -ls /user/jay/d//".!
// replace . with a whitespace, still work
```

## 10) quick create dataFrame from collection
```
val emp = Seq((101, "Amy", Some(2)),(100, "Jay", Some(5))
val employee = spark.createDataFrame(emp).toDF("employeeId","employeeName","managerId")
```
## 20) create DataSet from DataFrame
```
case class Employee(employeeId:Integer, employeeName:String, managerId:String) //something:Double, timestamp:Integer
val empDs = employee.as[Employee]
```
### 30) register UDF
```
 spark.udf.register("priceGroup", (p:Int ) => if (p > 1000) "High" else "Low")
```

### 40) get all configuration 
```
spark.conf.getAll
```

### 50)  Turn off processing log info in screen; go to log4j file, remove extension .template to make it a real config file, edit log4j.rootcategory=ERROR
```
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)
// or
spark.sparkContext.setLogLevel("ERROR") //ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
```

### 60) set Hive Dynamic Partition and Recreate table with Partition
```
// go back to Context to set Config
spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
```
### 70) Create external table, use file:///path for local file
```
sql(s"CREATE EXTERNAL TABLE table200(key BIGINT, value STRING) ROW FORMAT DELIMITED fields TERMINATED by ',' lines TERMINATED by '\\n' stored as TEXTFILE location '/home/ec2-user/hive_external' ")
```
