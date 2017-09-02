package com.learning.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//Asynchronous Send
// max.in.flight.requests.per.connection - limit to control how many messages we can send without ACk. By default is 5. 
// We can modify that limit.

public class AsynchronousProducer {
  
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
	  producer.send(record,new ProducerCallback());
      producer.close();
	  
	  System.out.println("SimpleProducer Completed.");
   }
}


class ProducerCallback implements Callback {

	public void onCompletion(RecordMetadata recordMetadata, Exception e) {
		if (e != null)
			System.out.println("AsynchronousProducer failed with an exception");
		else
			System.out.println("AsynchronousProducer call Success:");
	}

}   
    
    
    
    
    