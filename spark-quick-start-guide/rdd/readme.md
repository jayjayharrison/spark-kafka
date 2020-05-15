```
sudo find / > flist.txt # list all directory to flist.txt 
//hadoop fs -copyFromLocal flist.txt /user/jay
wc -l flist.txt
cat flist.txt | head -n10

spark-shell

val flistRDD = sc.textFile("flist.txt", 5) // split into 5 partition

flistRDD.count()

flistRDD.foreachPartition(p =>println("No of Items in partition-" + p.count(y=>true)))

flistRDD.getNumPartitions

val listRDD = flistRDD.map(x=> x.split("/")) // convertinto an array RDD

val kvRDD= listRDD.map(a => (a(0),1)) # convert the list into a tuple/key value pair. (first element, 1) ir. (bin,1)

val fcountRDD = kvRDD.reduceByKey( (x,y)=> x+y ) 

fcountRDD.collect() # return result RDD to driver

```
