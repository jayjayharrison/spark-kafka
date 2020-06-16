spark-shell --jars hadoop-aws-2.7.3.jar --conf spark.s3.access.key=AKIAW4... --conf spark.s3.secret.key=kYzfp...

val s3ak = spark.conf.get("spark.s3.access.key")

val s3sk = spark.conf.get("spark.s3.secret.key")

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAW45SKPH3ZKSL75OU")

spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
	 
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

val rdd = spark.sparkContext.textFile("s3a://my-test-bucket100/book.txt")

val df = spark.read.text("s3a://my-test-bucket100/book.txt")

val df = spark.write.text("s3a://my-test-bucket100/out")

df.write.text("s3a://my-test-bucket100/out")
