```
package com.alex.transform
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{struct, array, explode ,split, row_number,concat, col, lit, concat_ws, regexp_replace,trim,get_json_object,monotonically_increasing_id,lpad}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.expressions.Window
import com.alex.app.ScalaUtil.load_mapping_as_tuple
import scala.collection.{immutable, mutable}
import com.alex.transform.DataFrameExtensions.DataFrameJsonExtensions
import org.scalatest.enablers.Length
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.collection.mutable.ListBuffer

object DataFrameTransform{
  
  
  def assign_sk()(df: DataFrame): DataFrame = {
      val format = "yyMMddHH"
      val dtf = DateTimeFormatter.ofPattern(format)
      val tmnow = LocalDateTime.now().format(dtf)
      val df_with_row_num = df.withColumn("row_num", row_number() over Window.orderBy(col("landing_tmst")))
      val df_sk = df_with_row_num.select(concat(lit(tmnow),lpad(col("row_num"),5,"0")).cast(LongType).alias("sys_sk"), col("*"))
      val new_df = df_sk.drop(col("row_num"))
      return new_df
  }
  
  def orbat_json_to_string(mappingTuples: List[(String,String)])(df: DataFrame): DataFrame = {
      val select_function_list = mappingTuples
                .map( x => get_json_object( col("value"),"$." + x._2 ).alias( x._1 ) ) 
      val new_df = df.toJSON.select(select_function_list:_*)
      return new_df 
  }
  
  

  def mass_regexp_replace(pattern_replacement: List[(String,String)],  inclusion: List[String])(df: DataFrame): DataFrame = {
    val cols = df.columns
    pattern_replacement.foldLeft(df)((df_acc, replaceTuple) => {
        val pattern = replaceTuple._1
        val replacement = replaceTuple._2
        df_acc.select(cols.map( 
          c => c match{ 
          case c if inclusion.contains(c) =>  regexp_replace(regexp_replace(trim(col(c)), pattern, replacement),  "^[\\W]+$","").alias(c) 
          case _ => regexp_replace(trim(col(c)), "^[\\W]+$","").alias(c) }
          ): _* )
          
      }
    )
  }
  
 def mass_regexp_replace(pattern_replacement: List[(String,String)])(df: DataFrame): DataFrame = {
    val cols = df.columns
    pattern_replacement.foldLeft(df)((df_acc, replaceTuple) => {
        val pattern = replaceTuple._1
        val replacement = replaceTuple._2
        df_acc.select(cols.map( 
          c => c match{ 
          case _ =>  regexp_replace(trim(col(c)), pattern, replacement).alias(c) }
          ): _* )  
      }
    )
  }
  
  def explode_string_array_with_sep(col_name: String, sep: String)(df: DataFrame): DataFrame = {
    df.withColumn("new_name_6549845", split(col(col_name),sep))
      .withColumn(col_name, explode(col("new_name_6549845")))
      .withColumn(col_name, trim(col(col_name)))
      .drop("new_name_6549845")
  }
 
  def json_normalize()(df: DataFrame): DataFrame = {
    DataFrameJsonExtensions(df).flatten()
  }
  
  @Override
  def map_columns(mapping_path: String)(df: DataFrame): DataFrame = {
    val mappingTuples = load_mapping_as_tuple(mapping_path)
    
    val df_mapped = df.transform(map_columns(mappingTuples))
    return df_mapped
  }
  
  @Override
  def map_columns(mappingTuples: List[(String,String)])(df: DataFrame): DataFrame = {
    
    def firstNotNull = (col_value: String) => {
      col_value.split("\\|\\|").filterNot( _ == "").head.mkString
    } 
 
    val firstNotNullUDF = udf(firstNotNull)
    val srcColsAllList = mappingTuples.map( _._2 ).flatMap( c => c.split(",")).distinct
    val srcColsAllSet = srcColsAllList.toSet 
    
    val tgtColsAllList = mappingTuples.map( _._1 ).distinct
    val tgtColsAllSet =tgtColsAllList.toSet
    
    println("target columns: ")
    println(tgtColsAllList)
    
    val df_filled = df.transform(fill_missing_column(srcColsAllList))
    val dropColumns = srcColsAllSet.diff(tgtColsAllSet)
 
    val df_mapped = mappingTuples.foldLeft(df_filled)( (df_accu, colTuple)=> {
      
      val srcColsString = colTuple._2.split(",")
 

      val srcCols = srcColsString.map(col(_)).toList
 
      val tgtCol = colTuple._1
      
      val df_renamed = df_accu.withColumn(tgtCol, concat_ws("||", srcCols:_ *))
      
      srcCols.length match {
        case 1 => df_renamed
        case _ => df_renamed.withColumn(tgtCol, split(regexp_replace(col(tgtCol), "^(\\|{2,})|(\\|{2,})$", ""), "\\|\\|")(0))
      }

    }
    
    )
    val df_reordered = df_mapped.drop(dropColumns.toList:_*).select(tgtColsAllList.head, tgtColsAllList.tail:_ *)  
    return df_reordered
  }
  
  private def fill_missing_column(RequiredCols: List[String])(df: DataFrame): DataFrame  = {
      val srcCols = df.columns
      
      val allCols = srcCols ++ RequiredCols
      val col_list = allCols.distinct.map( c => c match {
                      case c if srcCols.contains(c) => col(c)
                      case _ => lit("").as(c)
                    } )     
     return df.select(col_list:_ *)               
  }
  
  def convert_to_json(mappingTuples: List[(String,String)])(df: DataFrame): DataFrame = {
    
    val srcColsAllList = mappingTuples.flatMap( _._2.split(",") ).distinct
    val df_filled = df.transform(fill_missing_column(srcColsAllList))

    val TransformRules = mappingTuples.groupBy(_._1).map( tp=> { val valueList = tp._2.map(_._2).toList; (tp._1, valueList.mkString(","))}).toList
    
    val new_df = TransformRules.foldLeft(df_filled)((df_accumulated, TransformRuleMap)=> {

  	var jsonStruct = new ListBuffer[org.apache.spark.sql.Column]()
  	val key = TransformRuleMap._1
  	val valueList = TransformRuleMap._2.split(",")
  	println(key)
  	println(valueList.mkString)
  
  	valueList.foreach( value => {
  		//val dataElement = struct(lit(value).alias("name"), trim(split(col(value)," ")(0)).alias("value"),trim(split(col(value)," ")(1)).alias("units"))
  		val dataElement = struct(lit(value).alias("name"), trim(col(value)).alias("value"))
  		jsonStruct += dataElement
  								}
  	          )
  	val new_df = df_accumulated.withColumn(key,array(jsonStruct.toList:_ *))
  	new_df.drop(valueList:_ *)
      }
      )
return new_df
 }
  
  def remove_publication_from_name(col_1: String, col_2: String)(df: DataFrame): DataFrame  = {
      return df.withColumn(col_1, UDFStore.col_minus_col(col(col_1), col(col_2)))
  }  
}

object UDFStore {
  
  
  def col_minus_col: UserDefinedFunction = {
    
    def col_minus_col_str(col_1: String, col_2: String): String = {
    return col_1.trim().replace(col_2.trim(), "") } 
   
    udf { (col_1: String, col_2: String) => col_minus_col_str(col_1, col_2) }
  }
  

}


```
