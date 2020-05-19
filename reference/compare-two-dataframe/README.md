### using union and group by
```
import org.apache.spark.sql.functions.lit

```




### using except methods
```
// return the record that are in df1 but not in df2 
val diffdf = df1.except(df2)

// extract the columns name as a array
val columns = df1.schema.fields.map(_.name)

// compare each column of df, noticed that it's df1 minus df2, if df1=(jay,jay,jay) and df2=(jay,j), the return will be null
// to really compare, do another df2.except(df1)
val colDif = columns.map(col => df1.select(col).except(df2.select(col)))

colDif.map(diff => {if(diff.count > 0) diff.show}) 
//colDif.map( x=> x.show )

```
