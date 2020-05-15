## Tips

#### quick create dataFrame from collection
val emp = Seq((101, "Amy", Some(2)),(100, "Jay", Some(5))
val employee = spark.createDataFrame(emp).toDF("employeeId","employeeName","managerId")

#### create DataSet from DataFrame
```
case class Employee(employeeId:Integer, employeeName:String, managerId:String) //something:Double, timestamp:Integer
val empDs = employee.as[Employee]
```
#### UDF
```
 spark.udf.register("priceGroup", (p:Int ) => if (p > 1000) "High" else "Low")
```

#### get all configuration 
```
spark.conf.getAll
```

#### Turn off processing log in screen
```
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder
.appName("SparkSessionExample") 
.master("local[1]")  //yarn-clinet  yarn-cluster  better off specify this in spark sumbit than in the scripts
.config("spark.sql.warehouse.dir", "target/spark-warehouse")
.enableHiveSupport()
.getOrCreate
spark.sparkContext.setLogLevel("ERROR") //to only print out error;or "WARM"; after SparkSession.builder
```

#### set Hive Dynamic Partition and Recreate table with Partition
```
// go back to Context to set Config
spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
```
#### Create external table, use file:///path for local file
```
sql(s"CREATE EXTERNAL TABLE table200(key BIGINT, value STRING) ROW FORMAT DELIMITED fields TERMINATED by ',' lines TERMINATED by '\\n' stored as TEXTFILE location '/home/ec2-user/hive_external' ")
```
