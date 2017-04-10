package com.ibm.kafkainputexample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by bharatviswanadham on 6/14/16.
 */
public class KafkaInputConsumer {
    private String topic;
    private Properties consumerProperties;
    private KafkaConsumer<String,String> consumer;
    private long messageCount;

    public KafkaInputConsumer(String topic, Properties consumerProperties) {
        this.topic = topic;
        this.consumerProperties = consumerProperties;
        consumer = new KafkaConsumer<String, String>(this.consumerProperties);
        messageCount = 0;
    }

    public void runConsumer() {
        try {
            int timeoutCount =0;
            //Consumer subscribing to topic
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                //Polling the consumer to get records in a topic. Here passed 100 milli seconds as timeout. . Time it must wait in poll method, if data is not available
                ConsumerRecords<String, String> records = consumer.poll(100);
                //Checking if the records retrived is empty
                if(records.isEmpty()) {
                    timeoutCount++;
                    System.out.println("No data to consume");
                }
                else {
                    //Iterating the consumer records, to extract the data from topic
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf(" key = %s, value = %s \n", record.key(), record.value());
                        messageCount++;
                    }
                    timeoutCount = 0;
                }

                // If kafka consumer has not received continously data for 5 seconds, having timeout and closing the kafka consumer
                // The logic here is each time consumer polls and wait for 100 seconds. So, continuously if this happened 50 times
                // i.e 50 * 100 = 5000 ms.
                // Ignoring the execution time of the code which is very minor here.
                if (timeoutCount == 50) {
                    System.out.println("Total No of Messages Consumed from the topic " + topic +" is " + messageCount);
                    System.out.println("Kafka Consumer Timeout, because no data is received from Kafka Topic");
                    break;
                }
            }
        }
        catch(Exception exception)
        {
            System.out.println(exception);
        }
        finally {
            consumer.close();
            System.out.println("Consumer Closed");
        }
    }

    public void setProperties(Properties properties) {
        this.consumerProperties=properties;
    }

    public Properties getProperties() {
        return this.consumerProperties;
    }
}
