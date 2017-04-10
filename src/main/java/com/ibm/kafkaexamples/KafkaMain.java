package com.ibm.kafkaexamples;


import java.util.Properties;
import java.util.Random;
import com.ibm.kafkainputexample.*;

import static java.lang.System.exit;

/**
 * Created by bharatviswanadham on 6/10/16.
 */
public class KafkaMain {
    public static void main(String args[]) {

        try {

            //topic Name
            String topicName = "simple-topic";

            //Setting Producer Properties

            Properties producerProperties = new Properties();

            //Specify list of kafka brokers, it need not be fully exhausted list of kafka brokers
            // If multiple kafka brokers are present, specify at least two. This will help even one of the kafka broker is failed,
            // it can get information from other server.
            //Before running the program, replace it with your kafka cluster server details
            producerProperties.put("bootstrap.servers", "hostname.ibm.com:6667");

            //Acknowledgments required by leader to receive the request is complete.
            // Here all is provided, that means all the replica servers should acknowledge that request is complete.
            producerProperties.put("acks", "all");

            //No of retries, need to be done if kafka producer failed to publish message
            // Here the property is set to 0, that means we are not retrying if producer fails to publish message
            producerProperties.put("retries", 0);

            // This parameter helps producer to batch records, and send them. This will help to improve the performance.
            // The batch.size parameter is in bytes
            producerProperties.put("batch.size", 10000);

            //producer batches the records to transmit , by waiting the time mentioned by the parameter.
            //All records which have arrived between the waiting time, will be batched and sent as a single request
            //The parameter is in milli seconds. Here we set to 1000 ms, which is 1 sec.
            producerProperties.put("linger.ms", 1000);

            //Total no of bytes producer can use to buffer the records.
            producerProperties.put("buffer.memory", 5242880);

            //Serializer Properties
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


            //Setting Consumer Properties
            Properties consumerProperties = new Properties();

            //Specify list of kafka brokers, it need not be fully exhausted list of kafka brokers
            // If multiple kafka brokers are present, specify at least two. This will help even one of the kafka broker is failed,
            // it can get information from other server.
            consumerProperties.put("bootstrap.servers", "hostname.ibm.com:6667");

            Random random = new Random();
            int groupId = random.nextInt();
            //Consumer group is the property which is used to identify, kafka consumer belongs to which group
            // Based on the group.id the delivery of messages is balanced between all consumers in that consumer group

            consumerProperties.put("group.id", "consumer-group-"+groupId);

            //consumer positions are committed automatically, for every auto.commit.interval.ms
            //Here, kafka take cares of consumer positions.
            consumerProperties.put("enable.auto.commit", "true");
            consumerProperties.put("auto.commit.interval.ms", "1000");

            //Consumers periodically send hear beat to servers. if they have not send heartbeats greater than session.timeout.ms,
            //server assume that consumer is dead, and reassigns its partitions to other process in consumer group
            consumerProperties.put("session.timeout.ms", "20000");

            // Deserializer properties
            consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            // This property is set to read the messages from the topic from starting offset, as generally kafka ignores the existing data
            // in topic, and reads the new incoming data to topic. To avoid this, resetting offset to earliest.
            consumerProperties.put("auto.offset.reset", "earliest");

            if(args.length == 0) {
                System.out.println("Invalid Option to run Program");
                System.out.println("Please use simple/input as option to run. Example Usage:target/kafka-simple-producer-consumer simple");
                exit(1);
            }
            switch(args[0])
            {
                case "simple":
                    System.out.println("#######################");
                    System.out.println("Running Kafka Producer");
                    KafkaSimpleProducer kafkaProducer = new KafkaSimpleProducer(topicName, producerProperties);
                    kafkaProducer.runProducer();
                    System.out.println("#######################");

                    System.out.println("#######################");
                    System.out.println("Running Kafka Consumer");
                    KafkaSimpleConsumer kafkaConsumer = new KafkaSimpleConsumer(topicName, consumerProperties);
                    kafkaConsumer.runConsumer();
                    System.out.println("#######################");
                    break;
                case "input":
                    System.out.println("#######################");
                    System.out.println("Running Kafka Producer, to publish user input to topic");
                    KafkaInputProducer kafkaInputProducer= new KafkaInputProducer("kafka-input-topic",producerProperties);
                    kafkaInputProducer.runProducer();
                    System.out.println("#######################");

                    System.out.println("#######################");
                    System.out.println("Running Kafka Consumer, to read messages from topic");
                    KafkaInputConsumer kafkaInputConsumer = new KafkaInputConsumer("kafka-input-topic", consumerProperties);
                    kafkaInputConsumer.runConsumer();
                    System.out.println("#######################");
                    break;
                default:
                    System.out.println("Not a Valid Option");
            }

            /*System.out.println("#######################");
            System.out.println("Running Kafka Producer");
            KafkaSimpleProducer kafkaProducer = new KafkaSimpleProducer(topicName, producerProperties);
            kafkaProducer.runProducer();
            System.out.println("#######################");

            System.out.println("#######################");
            System.out.println("Running Kafka Consumer");
            KafkaSimpleConsumer kafkaConsumer = new KafkaSimpleConsumer(topicName, consumerProperties);
            kafkaConsumer.runConsumer();
            System.out.println("#######################");


            System.out.println("#######################");
            System.out.println("Running Kafka Producer, to publish user input to topic");
            KafkaInputProducer kafkaInputProducer= new KafkaInputProducer("kafka-input-topic",producerProperties);
            kafkaInputProducer.runProducer();
            System.out.println("#######################");

            System.out.println("#######################");
            System.out.println("Running Kafka Consumer, to read messages from topic");
          *//*  groupId = random.nextInt();
            //Consumer group is the property which is used to identify, kafka consumer belongs to which group
            // Based on the group.id the delivery of messages is balanced between all consumers in that consumer group

            consumerProperties.put("group.id", "consumer-group-"+groupId);*//*
            KafkaInputConsumer kafkaInputConsumer = new KafkaInputConsumer("kafka-input-topic", consumerProperties);
            kafkaInputConsumer.runConsumer();
            System.out.println("#######################");*/

        }
        catch (Exception exception) {
            System.out.println(exception);
        }

    }
}
