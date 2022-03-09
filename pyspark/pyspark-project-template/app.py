import sys
from src.SparkSessionBuilder import SparkSessionBuilder
import findspark
findspark.init()

def main(args):
    """

    spark-submit --master spark://192.168.3.32:30077 --total-executor-cores 1 app.py

    :param args:
    :return:
    """
    spark = SparkSessionBuilder().getSparkSession()

    df = spark.createDataFrame(data=[{'name': 'Alice', 'age': 1}])

    df.show()



if __name__ == '__main__':
    main(sys.argv[1:])

