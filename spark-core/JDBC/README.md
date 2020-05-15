```
spark-shell --packages org.postgresql:postgresql:9.4.1207

// append to dbtable
dfout.write
  .format("jdbc")
  .mode("append")    //.mode("overwrite").option("truncate", "true")
  .option("driver", "org.postgresql.Driver")
  .option("url", "jdbc:postgresql://10.128.0.4:5432/sparkDB")
  .option("dbtable", "survey_results")
  .option("user", "jay")
  .option("password", "jay123")
  .save()

// read from table
val pgDF_table = spark.read
  .format("jdbc")
  .option("driver", "org.postgresql.Driver")
  .option("url", "jdbc:postgresql://10.128.0.4:5432/sparkDB")
  .option("dbtable", "select * from survey_results where lastmodifieddate > xxxxxx")
  .option("user", "jay")
  .option("password", "jay123")
  .load()
pgDF_table.show


```
jdbc shortcut api
````
def jdbc(url: String, table: String, columnName: String, lowerBound: Long, upperBound: Long, numPartitions: Int, connectionProperties: Properties): DataFrame
    Permalink
    Construct a DataFrame representing the database table accessible via JDBC URL url named table.
```
https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader

### other install postgres client on your machine

check your linux os info run:hostnamectl

psql --host=<servername> --port=<port> --username=<user@servername> --dbname=<dbname>
psql --host=jaypsql.postgres.database.azure.com --port=5432 --username=jaypsqladmin@jaypsql --dbname=postgres % ceated by default 

show hba_file; # PostgreSQL Client Authentication Configuration File and vi editor the path 

add below to the end of the file
host	all				all				0.0.0.0/0				md5


