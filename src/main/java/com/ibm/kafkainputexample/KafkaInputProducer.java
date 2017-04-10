package com.ibm.kafkainputexample;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.PrintWriter;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by bharatviswanadham on 6/14/16.
 */
public class KafkaInputProducer {

    private String topic;
    private Properties producerProperties;
    private KafkaProducer kafkaProducer;


    public KafkaInputProducer(String topicName, Properties producerProperties) {
        this.topic = topicName;
        this.producerProperties = producerProperties;
        kafkaProducer = new KafkaProducer<String,String>(producerProperties);
    }

    public void runProducer() {
        try {

            Scanner input = new Scanner(System.in);
            while (true) {
                System.out.println("Enter a Message to publish to kafka topic " + topic + "; to stop, enter message 'close' ");
                String message = input.nextLine();

                if (message.contains("close"))
                    break;
                else {
                    //Creating the producer record object with which topic it belongs to along with Message Key and Value
                    ProducerRecord record = new ProducerRecord<String, String>(topic, topic, message);
                    //Publishing the record to topic
                    kafkaProducer.send(record);
                    System.out.println("Published Message to " + this.topic);
                }

            }
        }
        catch(Exception ex)
        {
            System.out.println("Exception has Occurred:" + ex);
        }
        finally {
            kafkaProducer.close();
            System.out.println("Kafka Producer is Closed");
        }
    }

    public void setProducerProperties(Properties producerProperties) {
        this.producerProperties = producerProperties;
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }


}
