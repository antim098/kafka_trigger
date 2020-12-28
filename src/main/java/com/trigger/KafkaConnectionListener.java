package com.trigger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

public class KafkaConnectionListener extends TimerTask {
    private AdminClient kafkaAdmin;
    private Logger logger = LoggerFactory.getLogger(KafkaConnectionListener.class);

    public KafkaConnectionListener(AdminClient client) {
        this.kafkaAdmin = client;
    }

    @Override
    public void run() {
        try {
            kafkaAdmin.listTopics(new ListTopicsOptions().timeoutMs(5000)).listings().get();
            Trigger.setKafkaStatus(true);
            //logger.info("=========================Kafka Service is Running========================");
        } catch (Exception ex) {
            Trigger.setKafkaStatus(false);
            //logger.info("=================Kafka Service is not running, timed out after 5000 ms==============");
        }
    }
}
