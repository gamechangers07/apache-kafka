package com.learning.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

//Fire and forget Approach 
public class SimpleProducer {
  
   public static void main(String[] args) throws Exception{
           
      String topicName = "SimpleProducerTopic";
	  String key = "Key2";
	  String value = "Value-1";
      
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092,localhost:9093"); // brokers
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         //convert into bytes array
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        
      Producer<String, String> producer = new KafkaProducer <String, String>(props);
	
	  ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key,value);
	  producer.send(record);
      producer.close();
	  
	  System.out.println("SimpleProducer Completed.");
   }
}