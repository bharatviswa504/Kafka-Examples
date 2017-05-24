/**
 * Created by bharatviswanadham on 5/24/17.
 */

import java.io.InputStream;
import java.util.Properties;

public class KafkaMain {

    public static void main(String args[]) {
        try {
            //topic Name
            String topicName = "simple-topic2";



            String resourceName = "client.properties"; // could also be a constant
            ClassLoader loader = Thread.currentThread().getContextClassLoader();

            Properties producerProperties1 = new Properties();
            try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
                producerProperties1.load(resourceStream);
            }

             resourceName = "client.properties"; // could also be a constant

            Properties producerProperties2 = new Properties();
            try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
                producerProperties2.load(resourceStream);
            }

            System.out.println("#######################");
            System.out.println("Running Kafka Producer");
            SimpleProducer kafkaProducer1 = new SimpleProducer(topicName, producerProperties1);
            SimpleProducer kafkaProducer2 = new SimpleProducer(topicName, producerProperties2);
            Thread t1 = new Thread(kafkaProducer1);
            Thread t2 = new Thread(kafkaProducer2);
            t1.start();
            t2.start();

            t1.join();
            t2.join();

            System.out.println("#######################");
        }

catch (Exception exception) {
        System.out.println(exception);
        }

    }
}

