package com.trigger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Writer {
    public static BufferedWriter fileWriter;
    private static Logger logger = LoggerFactory.getLogger(Writer.class);

    public static BufferedWriter getWriter() {
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
        return fileWriter;
    }
}
