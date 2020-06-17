https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#:~:text=2.1%20text()%20%E2%80%93%20Read%20text,all%20files%20from%20a%20directory.
```
spark-shell --jars hadoop-aws-2.7.3.jar --conf spark.s3.access.key=AKIAW4... --conf spark.s3.secret.key=kYzfp...

val s3ak = spark.conf.get("spark.s3.access.key")

val s3sk = spark.conf.get("spark.s3.secret.key")

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "s3ak")

spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "s3sk+")
	 
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

val rdd = spark.sparkContext.textFile("s3a://my-test-bucket100/book.txt")

val df = spark.read.text("s3a://my-test-bucket100/book.txt")

val df = spark.write.text("s3a://my-test-bucket100/out")

df.write.text("s3a://my-test-bucket100/out")
```
