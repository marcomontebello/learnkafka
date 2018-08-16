package com.learnkafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerKafka {
	
	public static void main(String[] args) {
		Properties properties=new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		//serializer della chiave e del valore che verranno inviati come messaggio del producer verso il broker kafka
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String,String> myProducer= new KafkaProducer<String,String>(properties);
		
		try {

			for(int i=1;i<50;i++){
			
				
			    /** DIRECT PRODUCER PARTITIONING MECHANISM */
				myProducer.send(new  ProducerRecord<String, String>("replicated-and-partitioned-topic", 2, "message","Message Value : " + Integer.toString(i)));
				
			} 
			
				for(int i=50;i<100;i++){	
				 
				/** ROUND-ROBIN PRODUCER PARTITIONING MECHANISM - elimino numero partizione e chiave! */
				myProducer.send(new  ProducerRecord<String, String>("replicated-and-partitioned-topic","Message Value : " + Integer.toString(i)));
				
			}
				/**KEY-HASHING PRODUCER PARTITIONING MECHANISM - elimino numero partizione e chiave! */

				for(int i=101; i<150;i++){
					
					myProducer.send(new  ProducerRecord<String, String>("replicated-and-partitioned-topic","message_"+i,"Message Value : " + Integer.toString(i)));

					
				}
				
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			myProducer.close();
		}
	}

}
