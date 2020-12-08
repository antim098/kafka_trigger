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
            logger.info("===============Running kafka connection check=============");
            kafkaAdmin.listTopics(new ListTopicsOptions().timeoutMs(5000)).listings().get();
            Trigger.setKafkaStatus(true);
        } catch (Exception ex) {
            logger.info("=================Kafka is not available, timed out after 5000 ms==============");
            Trigger.setKafkaStatus(false);
        }
    }
}
