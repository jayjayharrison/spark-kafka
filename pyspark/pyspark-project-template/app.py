import sys
from src import SparkSessionBuilder2



def main(args):
    """
    spark-submit --master local[1] --total-executor-cores 1 app.py
    spark-submit --master spark://ip:port --total-executor-cores 1 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions app.py

    spark-submit --conf spark.hadoop.fs.s3a.endpoint=http://ip:port --conf spark.hadoop.fs.s3a.access.key=xxxx --conf spark.hadoop.fs.s3a.secret.key=xxx app.py
    :param args:
    :return:
    """
    spark = SparkSessionBuilder2().getSparkSession()


    df = spark.createDataFrame(data=[{'name': 'Alice', 'age': 1}])

    df.show()

    #spark.sql("SHOW NAMESPACES FROM mycatalog").show()

    df2 = spark.read.json(path='s3a://xxx/xxx')
    df2.show()

if __name__ == '__main__':
    main(sys.argv[1:])

