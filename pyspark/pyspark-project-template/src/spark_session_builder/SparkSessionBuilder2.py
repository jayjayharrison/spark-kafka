from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
from pyspark import SparkFiles
import json
import findspark
findspark.init()


class SparkSessionBuilder2:

    def __init__(self):
        self.this_dir = os.path.dirname(os.path.realpath(__file__))
        self.root_dir = os.path.dirname(os.path.dirname(self.this_dir))
        self.temp_dir = os.path.join(self.root_dir, 'tmp/spark/')


    def getSparkSession(self, files=[], jars=[]):
        """
        setting spark config in code will take precendence over setting them in spark-submit

        https://spark.apache.org/docs/latest/submitting-applications.html#loading-configuration-from-a-file

        :param app_name:
        :param master:
        :param jars:
        :param files:
        :param spark_config:
        :return:
        """
        src_jars_path = os.path.join(self.root_dir, 'jars')
        src_jars = [os.path.join(src_jars_path, f) for f in os.listdir(src_jars_path)]
        jars = ','.join(src_jars + jars)

        src_files_path = os.path.join(self.root_dir, 'files')
        src_files = [os.path.join(src_files_path, f) for f in os.listdir(src_files_path)]
        files = ','.join(src_files)


        conf = (SparkConf()
                .set("spark.jars", jars)
                .set("spark.files", files)
                .set("spark.local.dir", self.temp_dir)

                # .set("spark.hadoop.fs.s3a.endpoint", 'http://xxxx')
                # .set("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ACCESS_KEY_ID'))
                # .set("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_SECRET_ACCESS_KEY'))
                )

        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')
        spark_files_dir = SparkFiles.getRootDirectory()
        print(spark_files_dir)
        config_files = [os.path.join(spark_files_dir, filename)
                        for filename in os.listdir(spark_files_dir)
                        if filename.endswith('config.json')][0]

        with open(config_files, 'r') as f:
            config_map = json.load(f)

        hadoopConf = sc._jsc.hadoopConfiguration()


        for key, val in config_map.items():
            if 'hadoop' in key:
                sc._conf.set(key, val)
            else:
                sc._conf.set(key, val)

        spark = SparkSession(sc)

        print(spark.sparkContext.getConf().getAll())
        print(spark.conf.get("spark.cassandra.connection.host"))
        print(spark.conf.get("spark.hadoop.fs.s3a.access.key"))
        print(spark.conf)

        return spark
