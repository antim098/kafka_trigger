package com.trigger;
/**
 *
 */

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ExampleTrigger implements ITrigger {

    private String topic;
    private Producer<String, String> producer;
    private ThreadPoolExecutor threadPoolExecutor;

    /**
     *
     */
    public ExampleTrigger() {

        Thread.currentThread().setContextClassLoader(null);

        topic = "test";
        producer = new KafkaProducer<String, String>(getProps());
        threadPoolExecutor = new ThreadPoolExecutor(4, 20, 30,
                TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
    }

    /**
     *
     */
    @Override
    public Collection<Mutation> augment(Partition partition) {
        threadPoolExecutor.execute(() -> handleUpdate(partition));
        return Collections.emptyList();
    }

    /**
     *
     * @param partition
     */
    private void handleUpdate(Partition partition) {

        if (!partition.partitionLevelDeletion().isLive()) {
            return;
        }
        UnfilteredRowIterator it = partition.unfilteredIterator();
        while (it.hasNext()) {
            Unfiltered un = it.next();
            Row row = (Row) un;
            if (row.primaryKeyLivenessInfo().timestamp() != Long.MIN_VALUE) {
                Iterator<Cell> cells = row.cells().iterator();
                Iterator<ColumnDefinition> columns = row.columns().iterator();

                while (cells.hasNext() && columns.hasNext()) {
                    ColumnDefinition columnDef = columns.next();
                    Cell cell = cells.next();
                    if ("payload_json".equals(columnDef.name.toString())) {
                        producer.send(new ProducerRecord<>(
                                topic, columnDef.type.getString(cell.value())));
                        break;
                    }
                }
            }
        }
    }

    /**
     *
     * @return
     */
    private Properties getProps() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }
}