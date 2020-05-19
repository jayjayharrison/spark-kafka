
## 1) Remove trim space and initial cap in all columns
```
val raw_df = Seq(("jay", "sales", "sales manager ") , ("ali", "HR ","junior Recruiter "), ("maryk","I t","Web developer")).
              toDF("NAME", "DEPT name", "job title")

// df.columns => ARRAY(name, dept): this is only used to assign column name. you can be Seq("name", "dept").foldLeft  also
// foldleft(raw_df): start with raw_df, 
// resultDF is a placeholder for accumulative sum, colName is the ARRAY(name, dept) iteration used to fold left

val cleaned_df = raw_df.columns.foldLeft(raw_df) { 
  (resultDF, colName) =>
  resultDF.withColumn( colName, initcap(trim(raw_df(colName))) )
}

// resultDF.withColumn( colName, regexp_replace(raw_df(colName), "(^\\s)+|(\\s)+$", "") // trim all space chrater \s \t etc

cleaned_df.show()
```
## 2) Rename column name, replace space with _ and lowercase colmn name
```
val renamed_df = cleaned_df.columns.foldLeft(cleaned_df) { 
  (resultDF, colName) =>
  resultDF.withColumnRenamed( colName, colName.toLowerCase().replace(" ", "_" ) )
}
renamed_df.show

+-----+---------+----------------+
| name|dept_name|       job_title|
+-----+---------+----------------+
|  Jay|    Sales|   Sales Manager|
|  Ali|       Hr|Junior Recruiter|
|Maryk|      I T|   Web Developer|
+-----+---------+----------------+
```
## 3) Apply UDF
```
def snakeCaseColumns(df: DataFrame): DataFrame = {
  df.columns.foldLeft(df) { (resultDF, colName) =>
    resultDF.withColumnRenamed(colName, toSnakeCase(colName))
  }
}

def toSnakeCase(str: String): String = {
  str.toLowerCase().replace(" ", "_")
}

val df = sourceDF.transform(snakeCaseColumns)

```
   
