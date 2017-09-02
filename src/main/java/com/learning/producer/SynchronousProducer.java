package com.learning.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//Synchronous Send
public class SynchronousProducer {

	public static void main(String[] args) throws Exception {

		String topicName = "SimpleProducerTopic";
		String key = "Key2";
		String value = "Value-1";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9096,localhost:9097"); // brokers
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // convert into bytes
																								// array
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

		try {
			RecordMetadata metadata = producer.send(record).get();
			System.out.println(
					"Message is sent to Partition no " + metadata.partition() + " and offset " + metadata.offset());
			System.out.println("SynchronousProducer Completed with success.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("SynchronousProducer failed with an exception");
		} finally {
			producer.close();
		}

	}
}