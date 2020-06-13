import java.net.URL
import java.util.Properties

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}


import com.typesafe.config.ConfigFactory

object kafka_producer {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load
	// conf.getConfig(args(0)) for input argument  
    val envProps = conf.getConfig("dev")	  
	  
    val kafka_topic_name = envProps.getString("kafka.topic")
    val kafka_bootstrap_servers = envProps.getString("bootstrap.server")

    // ==============Streaming HTTP Request to API===================== 
    val meetup_rsvp_stream_api_endpoint = "http://stream.meetup.com/2/rsvps"

    val url = new URL(meetup_rsvp_stream_api_endpoint)
    val connection = url.openConnection()
	
    val jsonfactory_object = new JsonFactory(new ObjectMapper)
    val json_parser = jsonfactory_object.createParser(connection.getInputStream)
	// ==============Streaming HTTP Request to API===================== 
	
	
    val props = new Properties()
	  
    props.put("bootstrap.servers", kafka_bootstrap_servers)
    // using ProducerConfig. for easy reference and avoid typo
    // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_bootstrap_servers)
    // run ProducerConfig. tab,  you will see a list of producer properties :  BOOTSTRAP_SERVERS_CONFIG is just String:"bootstrap.servers"
    props.put("acks", "all") // ProducerConfig.ACKS_CONFIG = "acks"
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerExample")  
	  

    val kafka_producer = new KafkaProducer[String,String](props)
	  
    while (json_parser.nextToken() != null)
    {
      val message = json_parser.readValueAsTree().toString()
	    
      println(message)
      val producer_record_object = new ProducerRecord[String, String](kafka_topic_name, message)
      
      kafka_producer.send(producer_record_object)
    }
	
    kafka_producer.close()

	
	}
	}
