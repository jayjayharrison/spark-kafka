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
