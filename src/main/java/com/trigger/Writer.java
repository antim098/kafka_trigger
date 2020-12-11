package com.trigger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Writer {
    public static BufferedWriter fileWriter;

    public static BufferedWriter getWriter() {
        if (fileWriter == null) {
            try {
                fileWriter = new BufferedWriter(new FileWriter("/home/impadmin/triggerLogs/data.txt", true));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileWriter;
    }
}
