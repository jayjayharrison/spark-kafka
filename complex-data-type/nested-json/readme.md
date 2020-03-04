









```
val emp_info = """
  [
    {"name": "foo", "address": {"state": "CA", "country": "USA"},
"docs":[{"subject": "english", "year": 2016}]},
    {"name": "bar", "address": {"state": "OH", "country": "USA"},
"docs":[{"subject": "math", "year": 2017}]}
  ]"""

import org.apache.spark.sql.types._

val addressSchema = new StructType().add("state", StringType).add("country",
StringType)
val docsSchema = ArrayType(new StructType().add("subject",
StringType).add("year", IntegerType))
val employeeSchema = new StructType().add("name", StringType).add("address",
addressSchema).add("docs", docsSchema)

val empInfoSchema = ArrayType(employeeSchema)

empInfoSchema.json //see json representation of the struct schema

val empInfoStrDF = Seq((emp_info)).toDF("emp_info_str")
empInfoStrDF.printSchema
empInfoStrDF.show(false)

val empInfoDF = empInfoStrDF.select(from_json('emp_info_str,
empInfoSchema).as("emp_info"))
empInfoDF.printSchema

empInfoDF.select(struct("*")).show(false)

empInfoDF.select("emp_info.name", "emp_info.address",
"emp_info.docs").show(false)

empInfoDF.select(explode('emp_info.getItem("name"))).show

```
