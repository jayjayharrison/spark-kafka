```
package com.alex.transform

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import scala.collection.{immutable, mutable}
import scala.collection.Seq

object DataFrameExtensions {
   val class_attr = "attr"
  
  abstract class Visitor[T] {
    def visitPrimitiveType(field: StructField, value: T): T
    def visitStructType(value: T): StructTypeVisitor[T]
  }

  abstract class StructTypeVisitor[T] {
    def visitKey(name: String): Unit
    def visitValue(): Visitor[T]
    def visitValue(value: T): T
  }

  class DataFrameVisitor extends Visitor[DataFrame] {
    override def visitPrimitiveType(field: StructField, df: DataFrame): DataFrame = df

    override def visitStructType(df: DataFrame): StructTypeVisitor[DataFrame] = new DataFrameStructVisitor()
  }

  class DataFrameStructVisitor extends StructTypeVisitor[DataFrame] {
    private var dfname: String = ""

    override def visitKey(name: String): Unit = dfname = name

    override def visitValue(): Visitor[DataFrame] = new DataFrameVisitor()

    override def visitValue(df: DataFrame): DataFrame = df.withColumn(s"${dfname.replace('.','-')
                                                .replace("`", "")
                                                .replace(" ","_")
                                                //.replace(":","")
                                                .replace("'","")
                                                .replace(",","")
                                                .toLowerCase()}", F.col(s"$dfname"))
  }

  private def on_flatten(field: StructField, visitor: Visitor[DataFrame], df: DataFrame, dropColumns: mutable.ListBuffer[String], prefixes: Seq[String] = Seq.empty): DataFrame = {
    
    
    field.dataType match {
      
      case StringType => visitor.visitPrimitiveType(field, df)
      case StructType(subFields) => {

        val structTypeVisitor = visitor.visitStructType(df)

        val df_tmp = subFields.foldLeft(df)((df_acc, subField) => {
          val key = (prefixes :+ s"`${field.name}`" :+ s"`${subField.name}`").mkString(".")
          structTypeVisitor.visitKey(key)
          val subVisitor = structTypeVisitor.visitValue()
          val subDf = on_flatten(subField, subVisitor, df_acc, dropColumns, prefixes :+ field.name)
          structTypeVisitor.visitValue(subDf)
        })

        val dropColumn = (prefixes :+ field.name).mkString("-")
        dropColumns += dropColumn
        dropColumns += dropColumn.replace('.','-')
                                  .replace("`", "")
                                  .replace(" ","_")
                                  .replace(":","")
                                  .replace("'","")
                                  .replace(",","")
                                  .toLowerCase()
        
        df_tmp
      }
      case _ => visitor.visitPrimitiveType(field, df)
    }
  }

  protected def flatten(df: DataFrame) = {
    df.schema.fields.foldLeft(df)((df_acc, field) => {
      val dropColumns: mutable.ListBuffer[String] = mutable.ListBuffer()

      
      val flattened_df = on_flatten(field, new DataFrameVisitor(), df_acc, dropColumns)

     
      flattened_df.drop(dropColumns:_*)
    })
  }

  protected def json_normalize(df: DataFrame, record_path: String, meta: Seq[String] = Seq.empty): DataFrame = {
    val cols: Seq[Column] = meta.map(F.col(_)) :+ F.explode_outer(F.col(record_path)).alias(record_path)

    val explodeDf = df.select(cols: _*)

    flatten(explodeDf)
  }

  protected def count_values(df: DataFrame, record_path: String): DataFrame = {
    df.groupBy(record_path).count().orderBy(F.col(record_path).desc)
  }

  private def countAvail(df: DataFrame, field: StructField): Long = {
    val (col, dataType) = (s"`${field.name}`", field.dataType)

    dataType match {
      case StructType(_) => df.filter(s"$col is not null").count()
      case ArrayType(_, _) => df.filter(s"$col is not null and size($col) > 0").count()
      case StringType => df.filter(s"$col is not null and length($col) > 0").count()
      case _ => df.filter(s"$col is not null").count()
    }
  }

  protected def count_nonEmpty(df: DataFrame): Map[String, Long] = {
    df.schema.fields.par.foldLeft(immutable.Map.empty[String, Long])((acc, field) => {
      acc + (field.name -> countAvail(df, field))
    })
  }

  implicit class DataFrameJsonExtensions(df: DataFrame)
  {
    def flatten() = DataFrameExtensions.flatten(df)

    def json_normalize(record_path: String, meta: Seq[String] = Seq.empty) = DataFrameExtensions.json_normalize(df, record_path, meta)

    def count_values(record_path: String) = DataFrameExtensions.count_values(df, record_path)

    def count_nonEmpty(implicit spark: SparkSession) = {
      import spark.implicits._

      DataFrameExtensions.count_nonEmpty(df).toSeq.toDF("name", "count")
    }
  }
  
  
}


```
