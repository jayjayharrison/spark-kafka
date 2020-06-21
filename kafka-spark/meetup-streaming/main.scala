//package com.example.sparkproject

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import com.typesafe.config.ConfigFactory

import java.util.Properties
import java.sql.Timestamp
import java.io.File

object meetupStreaming{
  def main(args: Array[String]): Unit = {
    println("Stream Processing Application Started ...")
	
	 val spark = SparkSession.
      builder().
      //master(conf.getString("execution.mode")).
      appName("Get MeetUp Stream").
      getOrCreate()
	  
	spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")  
	
	import spark.implicits._
	
	/*============================pull properties============================*/
	val conf_file = new File("app.properties")
	val conf = ConfigFactory.parseFile(conf_file).getConfig("dev")
	
	val kafka_topic_name = conf.getString("kafka.topic")
    val kafka_bootstrap_servers = conf.getString("bootstrap.servers")
	
    val mysql_host_name = conf.getString("jdbc.mysql.host_name")
    val mysql_port_no = conf.getString("jdbc.mysql.port")
	
	//get credential from environment variable: export spark_jdbc_mysql_username=yourpassword 
    val mysql_user_name = System.getenv("spark_jdbc_mysql_username")
    val mysql_password = System.getenv("spark_jdbc_mysql_password")
	
    
	val mysql_database_name = conf.getString("jdbc.mysql.database")
    val mysql_driver_class = "com.mysql.jdbc.Driver"
    val mysql_table_name = conf.getString("jdbc.mysql.table")
    val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name
	/*=====================================================================*/

	
/**
 *  
    val mongodb_host_name = "localhost"
    val mongodb_port_no = "27017"
    val mongodb_user_name = "admin"
    val mongodb_password = "admin"
    val mongodb_database_name = "meetup_rsvp_db"
    val mongodb_collection_name = "meetup_rsvp_message_detail_tbl"
*/	
	
	
	/*=============Define Message Schema for Json parsing==================*/
	  
    val message_schema = StructType(Array(
      StructField("venue", StructType(Array(
        StructField("venue_name", StringType),
        StructField("lon", StringType),
        StructField("lat", StringType),
        StructField("venue_id", StringType)
      ))),
      StructField("visibility", StringType),
      StructField("response", StringType),
      StructField("guests", StringType),
      StructField("member", StructType(Array(
        StructField("member_id", StringType),
        StructField("photo", StringType),
        StructField("member_name", StringType)
      ))),
      StructField("rsvp_id", StringType),
      StructField("mtime", StringType),
      StructField("event", StructType(Array(
        StructField("event_name", StringType),
        StructField("event_id", StringType),
        StructField("time", StringType),
        StructField("event_url", StringType)
      ))),
      StructField("group", StructType(Array(
        StructField("group_topics", ArrayType(StructType(Array(
          StructField("urlkey", StringType),
          StructField("topic_name", StringType)
        )), true)),
        StructField("group_city", StringType),
        StructField("group_country", StringType),
        StructField("group_id", StringType),
        StructField("group_name", StringType),
        StructField("group_lon", StringType),
        StructField("group_urlname", StringType),
        StructField("group_state", StringType),
        StructField("group_lat", StringType)
      )))
    ))
	/*=====================================================================*/
		
	/*============================Read Stream============================*/	
	val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic_name)
      .option("startingOffsets", "latest")
	  .option("includeTimestamp", true)
      .load

	println("Printing Schema of transaction_detail_df: ")
    meetup_rsvp_df.printSchema()  	
	  
	val df_1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

	val df_2 = df_1.select(from_json(col("value"), meetup_rsvp_message_schema)
        .as("message_json"), col("timestamp"))
	
	// .* : breack up json one level down
	val df_3 = df_2.select("message_json.*", "timestamp")
	  
	  
	val df_4 = df_3.select(col("group.group_name"),
      col("group.group_country"), col("group.group_state"), col("group.group_city"),
      col("group.group_lat"), col("group.group_lon"), col("group.group_id"),
      col("group.group_topics"), col("member.member_name"), col("response"),
      col("guests"), col("venue.venue_name"), col("venue.lon"), col("venue.lat"),
      col("venue.venue_id"), col("visibility"), col("member.member_id"),
      col("member.photo"), col("event.event_name"), col("event.event_id"),
      col("event.time"), col("event.event_url")  )
	
	
/*	//========================Write to Mango DB========================
	val spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
    println("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri)

    df_4.writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
	  
        val batchDF_renamed = batchDF.withColumn("batch_id", lit(batchId))
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database

		batchDF_renamed.write
        .format("mongo")
        .mode("append")
        .option("uri", spark_mongodb_output_uri)
        .option("database", mongodb_database_name)
        .option("collection", mongodb_collection_name)
        .save()
		
      }.start()
	
*/	
	val df_aggregated = df_4.groupBy("group_name", "group_country",
      "group_state", "group_city", "group_lat", "group_lon", "response")
      .agg(count(col("response")).as("response_count"))
	

	//========================Write Count DF to MySQL========================  
	val mysql_props = new java.util.Properties
    mysql_props.setProperty("driver", mysql_driver_class)
    mysql_props.setProperty("user", mysql_user_name)
    mysql_props.setProperty("password", mysql_password)  
	  
	val mysqlStreamWriter =  df_aggregated.writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
	  
        val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))
		
        batchDF_1
		.write
		.mode("append")
		.jdbc(mysql_jdbc_url, mysql_table_name, mysql_properties)
    
	}
	
	mysqlStreamWriter.start()  
	
	  
	//========================Write Count DF to Console========================    
	val consoleStreamWriter = df_aggregated
      .writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")  

	val query = consoleStreamWriter.start()   
	
	query.awaitTermination()
	  
	
  }
}
