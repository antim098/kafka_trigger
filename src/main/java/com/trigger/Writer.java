package com.trigger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Writer {
    public static BufferedWriter fileWriter;

    public static BufferedWriter getWriter() throws IOException {
        if (fileWriter == null) {
            fileWriter = new BufferedWriter(new FileWriter("/home/impadmin/triggerLogs/data.txt", true));
        }
        return fileWriter;
    }
}
