from pyspark.sql import SparkSession
import os
from pyspark import SparkFiles
import json

class SparkSessionBuilder:

    def __init__(self):
        self.this_dir = os.path.dirname(os.path.realpath(__file__))
        self.root_dir = os.path.dirname(self.this_dir)
        self.temp_dir = os.path.join(self.root_dir, 'tmp/spark/')


    def getSparkSession(self, files=[], jars=[], spark_config={}):
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

        spark_builder = (SparkSession.builder
                         # .master(master)
                         # .appName(app_name)
                         # .config('spark.executor.memory', '1g')
                         # .config("spark.driver.memory", "1g")
                         # .config("spark.memory.offHeap.size", "1g")
                         .config("spark.jars", jars)
                         .config('spark.files', files)
                         .config("spark.hadoop.fs.s3a.endpoint", 'http://xxx.xxx.x.xx:xxxxx/')
                         .config("spark.hadoop.fs.s3a.access.key", os.environ.get('ACCESS_KEY_ID'))
                         .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('SECRET_ACCESS_KEY'))
                         .config("spark.local.dir", self.temp_dir)
                         )
        for key, val in spark_config.items():
            spark_builder.config(key, val)

        spark = spark_builder.getOrCreate()
        spark_files_dir = SparkFiles.getRootDirectory()

        config_files = [os.path.join(spark_files_dir, filename)
                        for filename in os.listdir(spark_files_dir)
                        if filename.endswith('config.json')][0]

        with open(config_files, 'r') as f:
            config_map = json.load(f)

        for key, val in config_map.items():

            spark.conf.set(key, val)


        print(spark.sparkContext.getConf().getAll())
        print(spark.conf.get("spark.cassandra.connection.host"))
        print(spark.conf)
        return spark
