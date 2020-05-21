### Using Union and Groupby to find duplicate
```
import org.apache.spark.sql.functions.lit

    // initiate source and target DF
val df1 = spark.read.csv("data") 
val df2 = spark.read.csv("data1")
    
val srcDf = df1.withColumn("src", lit("df1"))
val tgtDf = df2.withColumn("src", lit("df2"))

    // union source and target DF
val unionDf = srcDf.union(tgtDf)

    //extract column name
val all_col = unionDf.columns.toSeq              
val main_col = unionDf.columns.toSeq.dropRight(1) //drop the last column src_nm, use this for group by 

// val main_col = unionDf.columns.toSeq.filterNot( e => e == "src")

    // trim all column and covert null to empty string
    // this is a heavy operation, it initialized a new df from unionDf
val trimed_df = all_col.foldLeft(unionDf) { 
                (resultDf, colName) => 
                       resultDf.withColumn( colName, 
                                                  when(unionDf(colName).isNull, "")
                                                  .when(unionDf(colName) === "null", "")
                                                  .otherwise( trim(unionDf(colName))) 
                                           ) 
                                      }  

    // groupby 
    // : _* syntax => passing all Seq as varargs,  cannot use select(main_col: _*) directly, because first paramater of select(a, b* ) must be explicitly specified, thus (main_col.head, main_col.tail: _*)
    
val mismatchDf = trimed_df.groupBy(main_col.head, main_col.tail: _*).agg(count("*").alias("cnt"),max("src")).filter("cnt != 2").sort($"_c0".desc)

mismatchDf.show()

    // using map and : _* var argement, to select all column in Seq 
    // trimed_df.select( col.map( x=> unionDf(x)): _* ).show
    // filter($"count" =!= 2)                                                                        
    // trimed_df.groupBy(col.head, col.tail: _*).count().show

```


### using except methods, and apply on each Column individually, return a Array of DataSet
```
  // return the record that are in df1 but not in df2 
val diffdf = df1.except(df2)

  // extract the columns name as a array

val columns = df1.schema.fields.map(_.name)
  //columns: Array[String] = Array(_c0, _c1, _c2, _c3, src)

  // compare each column of df, noticed that it's df1 minus df2, if df1=(jay,jay,jay) and df2=(jay,j), the return will be null
  // to really compare, do another df2.except(df1)
  
val colDif = columns.map(col => df1.select(col).except(df2.select(col)))

colDif.map(diff => {if(diff.count > 0) diff.show}) 
  //colDif.map( x=> x.show )

```
