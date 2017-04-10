package com.ibm.kafkaexamples;

/**
 * Created by bharatviswanadham on 6/10/16.
 */

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSimpleProducer {

    private String topic;
    private Properties producerProperties;
    private KafkaProducer<String,String> producer;


    public KafkaSimpleProducer(String topicName,Properties properties) {
        this.topic = topicName;
        this.producerProperties = properties;
        this.producer = new KafkaProducer<>(producerProperties);
    }

    public void runProducer() {
      try {
          for(int i = 0; i < 1000; i++) {
              //Creating the producer record object with which topic it belongs to along with Message Key and Value
              // Here placing both key and value as same like Message0 , Message2 , ....... Message999

              // Key and Value for topic
              String key = "Message "+Integer.toString(i);
              String value = "Message" + Integer.toString(i);
              ProducerRecord record = new ProducerRecord<String,String>(this.topic, key, value);
              //Publishing the record to topic
              producer.send(record);
              System.out.println("Published Message to " + this.topic);
          }
      }
      catch(Exception exception) {
          System.out.println(exception);
      }
      finally
      {
          producer.close();
          System.out.println("Kafka Producer Closed");
      }
    }


    public void setProperties(Properties properties) {
        this.producerProperties=properties;
    }

    public Properties getProperties() {
        return this.producerProperties;
    }

}
