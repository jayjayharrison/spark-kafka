https://dzone.com/articles/java-amp-apache-spark-for-data-quality-amp-validat
```
//Data Accuracy 

//Null Count
df.where(col("colname").isNull).count()

//specific Count
df.where(col("colname").===("somevalue")).count()

//schema validation
    for (elem <- df.schema) {
        if (elem.dataType != "ExpectedDataType") {
            // Print Error
        }
      }

//Duplicates Column Value
    val df1 = df.groupBy("colname").count()
    val df2 = df1.filter("count = 1")
println("No of duplicate records : "   + (df1.count() - df2.count()).toString())


//Uniqueness check
val df1 = df.groupBy("colname").count()
df1.filter("count = 1").count()


// negative check

df.where(col("columnname").===("female")).count()
or 
df.where(col("columnname").rlike("f*l*e")).count()


```
