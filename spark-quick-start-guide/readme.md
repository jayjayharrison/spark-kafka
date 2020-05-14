### turn off processing log in screen
val spark = SparkSession.builder().config("spark.master", "local[1]").appName("TestLog").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") //"WARM"
