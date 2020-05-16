### Create RDD of complex type
#### rdd can be join on key of  rdd with tuple(k, v)
#### if V contain multiple element, you must map V as a its own collection,  ex, (K, (v1,v2,v3))
#### k is treated as primary key to join
#### to filter value, ex, map( tuple =>  tuple._2._1 == sometime)  // tuple._2  is the value tuple

```
1,Jay,25,true
2,Amy,22,true
3,Ali,26,false

case class demographic(Name:String, Age:Int, isActive:Boolean)

val rdd =sc.textFile("demo.csv")

val arr_rdd = rdd.map( x=> x.trim.split(","))

val demo_rdd = arr_rdd.map( x => (x(0).toInt, demographic(  x(1).toString, x(2).toInt, x(3).toString.toBoolean ))) 
//tuple(id, demographic())

demo_rdd.take(3)
// res44: Array[(Int, demographic)] = Array((1,demographic(Jay,25,true)), (2,demographic(Amy,22,true)), (3,demographic(Ali,26,false)))

1,"Premium",58.5
2,"Silver",42.5
3,"Standard",20
case class planType( planType:String , price:Double )

val plan_rdd = sc.parallelize(Seq((1,"Premium",58.5),(2,"Silver",42.5),(3,"Standard",20)))  // (int, string, anyVal)

//price are mapped to AnyVal type, so need to convert to double
val plan_rdd2 = plan_rdd.map(tup => (tup._1, planType(tup._2, tup._3.asInstanceOf[Number].doubleValue) ) )

val join_rdd = demo_rdd.join(plan_rdd2)
res51: Array[(Int, (demographic, planType))] = Array((1,(demographic(Jay,25,true),planType(Premium,58.5))))

// using filter before join
demo_rdd.filter( t => t._2.Name == "Jay" ).join(plan_rdd2).take(3))
res54: Array[(Int, (demographic, planType))] = Array((1,(demographic(Jay,25,true),planType(Premium,58.5))))

```
