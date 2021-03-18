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

/**
 * Trigger class
 * Class implementing the ITrigger interface.
 * This class is instantiated when creating trigger.
 */
public class Trigger implements ITrigger {
    private static Logger logger = LoggerFactory.getLogger(Trigger.class);
    private static boolean isKafkaAlive = true;
    private static Timer timer;
    private static Properties properties = new Properties();
    private static AdminClient client;
    private Producer<String, String> producer;
    private ThreadPoolExecutor threadPoolExecutor;
    private String topic;


    /**
     * Trigger Constructor
     * Trigger object is created(constructor called) when creating trigger on cassandra table
     * Constructor initializes Kafka connections and schedules threads
     */
    public Trigger() {
        Thread.currentThread().setContextClassLoader(null);
        getProps();
        topic = properties.getProperty("topic");
        logger.info("Kafka Properties : " + properties);
        logger.info("topic : " + topic);
        producer = new KafkaProducer<String, String>(properties);
        client = AdminClient.create(properties);

        //Ensures that timer object accepts only one task in its queue, even in case of multiple triggers,
        //only the trigger which is created first schedules the task
        if (timer == null) {
            timer = new Timer();
            logger.info("Created timer object: " + timer);
            timer.schedule(new KafkaConnectionListener(client), 0, 60000);
            logger.info("=======Started KafkaConnectionListener=====");
        }
        threadPoolExecutor = new ThreadPoolExecutor(1, 1, 30,
                TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
    }

    /**
     * Return boolean value indicating kafka state(up or down)
     *
     * @return isKafkaAlive
     */
    static boolean getKafkaStatus() {
        return isKafkaAlive;
    }

    /**
     * Sets the boolean value for kafka state in isKafkaAlive variable
     *
     * @param value the value to be set
     */
    static void setKafkaStatus(boolean value) {
        isKafkaAlive = value;
    }

    /**
     * Loads the Kafka related properties, used when creating producer
     */
    private static void getProps() {
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("max.block.ms", "10000");
        properties.put("client.id", "Cassandra-Trigger-Producer");
        FileReader reader = null;
        try {
            reader = new FileReader("/etc/cassandra/conf/triggers/trigger.properties");
            properties.load(reader);
            logger.info("===============Properties Loaded==============");
        } catch (FileNotFoundException e) {
            logger.info("===============Properties File Not Found==============");
        } catch (IOException e) {
            logger.info(e.getMessage(), e);
        }
    }

    /**
     * The augment method is called for every insert/update/delete event on the table where trigger exists.
     * The method is overriden to define custom functionality on these events.
     * Call to augment method is made asynchronous using threadpool
     *
     * @param partition contains the partition records (single or multiple)
     * @return empty list indicating no modification in the original data
     */
    @Override
    public Collection<Mutation> augment(Partition partition) {
        threadPoolExecutor.submit(new TriggerThread(producer, partition, topic));
        return Collections.emptyList();
    }


    // FileWriter block
    /**
     * private static void createFileWriter() {
     * if (fileWriter == null) {
     * File file = new File("/etc/cassandra/conf/triggers/data.txt");
     * try {
     * if (!file.exists()) file.createNewFile();
     * fileWriter = new BufferedWriter(new FileWriter(file, true));
     * } catch (IOException e) {
     * logger.info("============Error while creating writer========");
     * logger.error("ERROR", e.getMessage(), e);
     * }
     * }
     * }
     */
/**
 @Override
 // finalize method is called on object once
 // before garbage collecting it
 protected void finalize() throws Throwable {
 logger.info("Object garbage collected : " + this);
 logger.info("Terminating associated timer thread");
 timer.cancel();

 }
 */

}
