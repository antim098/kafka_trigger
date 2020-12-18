package com.trigger;

import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class Writer {
    private static Properties properties = new Properties() ;
    private static Logger logger = LoggerFactory.getLogger(Writer.class);

    public static Properties loadProperties() {
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("max.block.ms", "15000");
        InputStream stream = Writer.class.getClassLoader().getResourceAsStream("/etc/cassandra/conf/triggers/Trigger.properties");
        try {
            properties.load(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            FileUtils.closeQuietly(stream);
        }
        return properties;
    }

}
