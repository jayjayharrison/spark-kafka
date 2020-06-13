import java.util.{Collections, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._

import play.api.libs.json._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
	  
 val conf = ConfigFactory.load
    val envProps = conf.getConfig("dev")	  
	  
    val kafka_topic_name = envProps.getString("kafka.topic")
    val kafka_bootstrap_servers = envProps.getString("bootstrap.server")
	  
    val props = new Properties()
	
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_bootstrap_servers)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaConsumerExample")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "1")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest") //last offset per group id , other: earliest

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(kafka_topic_name))
	
    while(true){
	    
      val records = consumer.poll(500)
	     
      for (record <- records.iterator()) {
	
	val rawJson = record.value()      
	      
	val jsonParser =  Json.parse(rawJson)     
	val group_city = jsonParser.\("group").\("group_city").as[String]     
	val group_name = jsonParser.\("group").\("group_name").as[String]
	val group_topic_list =  jsonParser.\("group").\("group_topics").as[List[Map[String,String]]]          
	val group_topics =  group_topic_list.map( m => m("topic_name")).mkString(",")  
	      
         println("Received: " + group_name + ", Topics: [" + group_topics + "] , fafka_offset:" + record.offset() + 
        ", kafka_timestamp:" + record.timestamp)
      
      }
    }
  
  }
}
