## Connect to S3
```
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "username")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "password")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://xx.xx.x.xx:xxxxx")

val df = spark.read.csv("s3a://bucket/path/")
// read with wildcard using *
val df = spark.read.csv("s3a://bucket/path/file_name_*")
```
