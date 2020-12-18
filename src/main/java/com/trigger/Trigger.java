package com.trigger;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Trigger implements ITrigger {
    private static boolean isKafkaAlive = true;
    private static Logger logger;
    private static Producer<String, String> producer;
    private static AdminClient client;
    private static Timer timer = new Timer();
    private static Properties properties = new Properties();
    private ThreadPoolExecutor threadPoolExecutor;
    private String topic;

    /**
     *
     */
    public Trigger() {
        getProps();
        Thread.currentThread().setContextClassLoader(null);
        //topic = "trigger";
        logger.info("===============Calling get properties==============");
        logger.info(properties.toString());
        topic = properties.getProperty("topic");
        producer = new KafkaProducer<String, String>(properties);
        logger.info("===============Producer Created==============");
        logger = LoggerFactory.getLogger(Trigger.class);
        client = AdminClient.create(properties);
        timer.schedule(new KafkaConnectionListener(client), 0, 60000);
        threadPoolExecutor = new ThreadPoolExecutor(1, 1, 30,
                TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
    }

    /**
     * @return
     */
//    private Properties getProps() {
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "10.105.22.175:9092");
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("max.block.ms", "15000"); //Time for which the producer will block on send() method
//        //properties.put("reconnect.backoff.ms", "1800000"); // Producer reconnect time to delay frequent reconnection
//        //properties.put("request.timeout.ms", "1800000"); //Adminclient reconnect time to delay frequent reconnection
//        return properties;
//    }
    private void getProps() {
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("max.block.ms", "15000");
        FileReader reader = null;
        try {
            reader = new FileReader("/etc/cassandra/conf/triggers/Trigger.properties");
            properties.load(reader);
            logger.info("===============Properties Loaded==============");
        } catch (FileNotFoundException e) {
            logger.info("===============File Not Found==============");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static boolean getKafkaStatus() {
        return isKafkaAlive;
    }

    static synchronized void setKafkaStatus(boolean value) {
        isKafkaAlive = value;
    }

    static Logger getLogger() {
        return logger;
    }

    /**
     *
     */
    @Override
    public Collection<Mutation> augment(Partition partition) {
        threadPoolExecutor.submit(new TriggerThread(producer, partition, topic));
        return Collections.emptyList();
    }

    // FileWriter block
    /**
     private static void createFileWriter() {
     if (fileWriter == null) {
     File file = new File("/etc/cassandra/conf/triggers/data.txt");
     try {
     if (!file.exists()) file.createNewFile();
     fileWriter = new BufferedWriter(new FileWriter(file, true));
     } catch (IOException e) {
     logger.info("============Error while creating writer========");
     logger.error("ERROR", e.getMessage(), e);
     }
     }
     }
     */

}
