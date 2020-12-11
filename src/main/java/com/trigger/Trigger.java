package com.trigger;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Trigger implements ITrigger {

    public static boolean isKafkaAlive = false;
    private static Logger logger = null;
    private String topic;
    private Producer<String, String> producer;
    private ThreadPoolExecutor threadPoolExecutor;
    private AdminClient client;
    private BufferedWriter fileWriter;
    private Timer timer = new Timer();

    /**
     *
     */
    public Trigger() {
        Thread.currentThread().setContextClassLoader(null);
        topic = "trigger";
        producer = new KafkaProducer<String, String>(getProps());
        logger = LoggerFactory.getLogger(Trigger.class);
        //client = AdminClient.create(getProps());
        //timer.schedule(new KafkaConnectionListener(client), 0, 60000);
        threadPoolExecutor = new ThreadPoolExecutor(1, 1, 30,
                TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        fileWriter = Writer.getWriter();
        if(fileWriter==null) logger.info("==========writer is null in Trigger Class========");
        try {
            fileWriter.write("testing");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static boolean getKafkaStatus() {
        return isKafkaAlive;
    }

    static void setKafkaStatus(boolean value) {
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
        threadPoolExecutor.submit(new TriggerThread(producer, partition, topic, fileWriter));
        //threadPoolExecutor.execute(() -> handleUpdate(partition));
        return Collections.emptyList();
    }

    /**
     * @return
     */
    private Properties getProps() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.105.22.175:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("reconnect.backoff.ms", "1800000");
        properties.put("request.timeout.ms", "1800000");
        return properties;
    }

}
