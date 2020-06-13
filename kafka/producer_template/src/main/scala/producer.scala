import java.net.URL
import java.util.Properties

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object kafka_producer {

  def main(args: Array[String]): Unit = {
    println("Kafka Producer Application Started ...")

    val kafka_topic_name = "test"
    val kafka_bootstrap_servers = "localhost:9092"

    // ==============Streaming HTTP Request to API===================== 
    val meetup_rsvp_stream_api_endpoint = "http://stream.meetup.com/2/rsvps"

	val url_object = new URL(meetup_rsvp_stream_api_endpoint)
    val connection_object = url_object.openConnection()
	
    val jsonfactory_object = new JsonFactory(new ObjectMapper)
    val parser_object = jsonfactory_object.createParser(connection_object.getInputStream)
	// ==============Streaming HTTP Request to API===================== 
	
	
	val properties_object = new Properties()
    properties_object.put("bootstrap.servers", kafka_bootstrap_servers)
    properties_object.put("acks", "all")
    properties_object.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties_object.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties_object.put("enable.auto.commit", "true")
    properties_object.put("auto.commit.interval.ms", "1000")
    properties_object.put("session.timeout.ms", "30000")

    val kafka_producer_object = new KafkaProducer[String,String](properties_object)
    while (parser_object.nextToken() != null)
    {
      val message_record = parser_object.readValueAsTree().toString()
      println(message_record)
      val producer_record_object = new ProducerRecord[String, String](kafka_topic_name, message_record)
      
	  kafka_producer_object.send(producer_record_object)
    }
	
    kafka_producer_object.close()

    println("Kafka Producer Application Completed.")
	
	}
	}
