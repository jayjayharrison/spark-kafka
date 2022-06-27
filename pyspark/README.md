## pyspark.sql.functions.input_file_name()
```
from pyspark.sql import functions as F
spark = get_spark_session()
df = (spark.read.parquet(path).withColumn("date_ingested", F.current_timestamp()).withColumn("input_file_name", F.input_file_name()))
```

## Pyspark in Jupyter notebook- PYSPARK_SUBMIT_ARGS

```
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4 pyspark-shell'
```

## Configuration for S3 connection
```
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set(‘fs.s3a.access.key’, accessKeyId)
hadoopConf.set(‘fs.s3a.secret.key’, secretAccessKey)
hadoopConf.set(‘fs.s3a.endpoint’, ‘s3-us-east-2.amazonaws.com’)
hadoopConf.set(‘fs.s3a.impl’, ‘org.apache.hadoop.fs.s3a.S3AFileSystem’)

```

## Spark File Streaming Quick Example
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("stream-app").master("local[2]").getOrCreate()


schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('fn', StringType(), True),
            StructField('ln', StringType(), True)
        ]
)

people = spark.readStream.format("csv").schema(schema).option("header",False).option("maxFilesPerTrigger", 1).load("data/")

people.printSchema()

sq = people.writeStream.format("json").outputMode("append").option("checkpointLocation", 'data-out-checkpoint/').start("data-out/")

sq.stop()

```
