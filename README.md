# spark

sudo find / > flist.txt                # list all directory to flist.txt
hadoop fs -ls /user

hadoop fs -copyFromLocal flist.txt /user/jay
hadoop fs -ls /user/jay

spark-shell

val flistRDD = sc.textFile("/user/jay/filist.txt", 5)   # split into 5 partition
val listRDD = flistRDD.map(x=> x.split("/"))            # convertinto an array RDD

val kvRDD= listRDD.map(a => (a(0),1))                   # convert the list into a tuple/key value pair. (first element, 1) ir. (bin,1)

val fcountRDD = kwRDD.reduceByKey( (x,y)=> x+y )        # 

fcountRDD.collect()                                     # return result RDD to driver 
