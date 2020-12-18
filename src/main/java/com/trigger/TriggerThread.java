package com.trigger;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

public class TriggerThread implements Callable<Object> {
    org.slf4j.Logger logger;
    private Partition partition;
    private Producer<String, String> producer;
    private String topic;

    public TriggerThread(Producer<String, String> producer, Partition partition, String topic) {
        this.producer = producer;
        this.partition = partition;
        this.topic = topic;
        this.logger = Trigger.getLogger();
    }

    @Override
    public Object call() {
        if (partitionIsDeleted(partition)) {
            return null;
        }
        String tableName = partition.metadata().cfName;
        List<ColumnDefinition> partitionColumns = partition.metadata().partitionKeyColumns();
        List<ColumnDefinition> clusteringColumns = partition.metadata().clusteringColumns();
        String key = getKey(partition);
        String[] partitionKeys = key.split(":");
        JSONObject partitionColsJson = new JSONObject();
        try {
            for (int i = 0; i < partitionColumns.size(); i++) {
                partitionColsJson.put(partitionColumns.get(i).toString(), partitionKeys[i]);
            }
        } catch (Exception ex) {
            logger.info("===================Error in parittion columns==============");
            logger.error("ERROR", ex.getMessage(), ex);
        }
        List<JSONObject> rows = new ArrayList<>();
        UnfilteredRowIterator it = partition.unfilteredIterator();
        while (it.hasNext()) {
            Unfiltered un = it.next();
            if (un.isRow()) {
                JSONObject jsonRow = new JSONObject();
                JSONObject payload = new JSONObject();
                Clustering clustering = (Clustering) un.clustering();
                String clusteringKey = clustering.toCQLString(partition.metadata());
                String[] clusteringKeys = clusteringKey.split(":");
                try {
                    for (int i = 0; i < partitionColumns.size(); i++) {
                        jsonRow.put(clusteringColumns.get(i).toString(), clusteringKeys[i]);
                    }
                } catch (Exception ex) {
                    logger.info("===================Error in clustering columns==============");
                    logger.error("ERROR", ex.getMessage(), ex);
                }
                //jsonRow.put("raw_ts", clusteringKey);
                Row row = partition.getRow(clustering);
                if (isInsert(row)) {
                    if (rowIsDeleted(row)) {
                        jsonRow.put("rowDeleted", true);
                    } else {
                        Iterator<Cell> cells = row.cells().iterator();
                        Iterator<ColumnDefinition> columns = row.columns().iterator();
                        while (cells.hasNext() && columns.hasNext()) {
                            ColumnDefinition columnDef = columns.next();
                            Cell cell = cells.next();
                            jsonRow.put(columnDef.name.toString(), columnDef.type.getString(cell.value()));
                        }
                        jsonRow.put("table", tableName);
                        jsonRow.putAll(partitionColsJson);
//                            jsonRow.put("ds", keys[0]);
//                            jsonRow.put("fallout_name", keys[1]);
//                            jsonRow.put("reason", keys[2]);
                    }
                    payload.put("payload", jsonRow);
                    rows.add(payload);
                }
            }
        }
        String value = rows.toString();
        logger.info("============================" + value);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        try {
            if (Trigger.getKafkaStatus()) {
                producer.send(record);
            } else {
                //Sending records to file in case kafka is down.
                //fileWriter.write("\n" + value);
            }
        } catch (Exception ex) {
            Trigger.setKafkaStatus(false);
            logger.info("===================Exception while sending record to producer==============");
            logger.error("ERROR", ex.getMessage(), ex);
            //fileWriter.write("\n" + value);
        }
        return null;
    }

    private boolean isInsert(Row row) {
        return row.primaryKeyLivenessInfo().timestamp() != Long.MIN_VALUE;
    }

    private String getKey(Partition partition) {
        return partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
    }

    private boolean partitionIsDeleted(Partition partition) {
        return partition.partitionLevelDeletion().markedForDeleteAt() > Long.MIN_VALUE;
    }

    private boolean rowIsDeleted(Row row) {
        return row.deletion().time().markedForDeleteAt() > Long.MIN_VALUE;
    }
}

