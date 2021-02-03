package com.trigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * TriggerThread Class
 * Provides custom functionality for handling the incoming data from augment method.
 */
public class TriggerThread implements Callable<Object> {
    public static final ObjectMapper MAPPER = new ObjectMapper();
    private static Logger logger = LoggerFactory.getLogger(TriggerThread.class);
    private Partition partition;
    private Producer<String, String> producer;
    private String topic;

    public TriggerThread(Producer<String, String> producer, Partition partition, String topic) {
        this.producer = producer;
        this.partition = partition;
        this.topic = topic;
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
        //key = 20201205:domino_entity_name:
        logger.info("Partition Key ==" + key);
        String[] partitionValues = key.split(":", -1);
        ObjectNode partitionColsJson = MAPPER.createObjectNode();
        //Flattening all the partition Columns and creating JSON
        for (int i = 0; i < partitionValues.length; i++) {
            partitionColsJson.put(partitionColumns.get(i).toString(), partitionValues[i]);
        }
        logger.info("Partition Key Json  " + partitionColsJson.toString());
        List<ObjectNode> rows = new ArrayList<>();
        UnfilteredRowIterator it = partition.unfilteredIterator();
        //JSONObject payload = new JSONObject();
        while (it.hasNext()) {
            Unfiltered un = it.next();
            if (un.isRow()) {
                ObjectNode payload = MAPPER.createObjectNode();
                ObjectNode jsonRow = MAPPER.createObjectNode();
                Clustering clustering = (Clustering) un.clustering();
                String clusteringKey = clustering.toCQLString(partition.metadata());
                logger.info("clusterkey " + clusteringKey);
                String[] clusteringKeys = clusteringKey.split(", ", -1);
                logger.info(clusteringKeys.toString());
                //Flattening all the clustering Columns and adding to JSON row object
                for (int i = 0; i < clusteringKeys.length; i++) {
                    jsonRow.put(clusteringColumns.get(i).toString(), clusteringKeys[i]);
                }
                logger.info("cluster key json " + jsonRow.toString());
                Row row = partition.getRow(clustering);
                if (isInsert(row)) {
                    if (rowIsDeleted(row)) {
                        jsonRow.put("rowDeleted", true);
                    } else {
                        //int counter = 0;
                        Iterator<Cell> cells = row.cells().iterator();
                        Iterator<ColumnDefinition> columns = row.columns().iterator();
                        while (cells.hasNext() && columns.hasNext()) {
                            ColumnDefinition columnDef = columns.next();
                            Cell cell = cells.next();
                            //jsonRow.put(columnDef.name.toString(), columnDef.type.getString(cell.value()));
                            String value = columnDef.type.getString(cell.value()).trim();
                            if (!value.equals("NULL") && !value.equals("")) {
                                jsonRow.put(columnDef.name.toString(), value);
                            }
                        }
                        jsonRow.put("table", tableName);
                        logger.info("Json Row " + jsonRow.toString());
                        jsonRow.putAll(partitionColsJson);
                    }
                    payload.put("payload", jsonRow);
                    logger.info("Full payload " + payload.toString());
                    rows.add(payload);
                }
            }
        }
        try {
            if (Trigger.getKafkaStatus()) {
                producer.send(new ProducerRecord<String, String>(topic, key, rows.toString()));
//            } else {
//                //Sending records to file in case kafka is down.
//                //fileWriter.write("\n" + value);
//            }
            }
        } catch (Exception ex) {
            Trigger.setKafkaStatus(false);
            logger.info("===================Exception while sending record to producer==============");
            logger.info(ex.getMessage(), ex);
        }
        return null;
    }

    /**
     * Checks if the row came as an
     * insert operation based on the liveness info
     *
     * @param row
     * @return boolean
     */
    private boolean isInsert(Row row) {
        return row.primaryKeyLivenessInfo().timestamp() != Long.MIN_VALUE;
    }

    /**
     * Extracts the partition key from the partition
     *
     * @param partition
     * @return partition key string concatenated with : in case of multiple values
     */
    private String getKey(Partition partition) {
        return partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
    }

    /**
     * Checks if the partition was deleted
     *
     * @param partition
     * @return boolean
     */
    private boolean partitionIsDeleted(Partition partition) {
        return partition.partitionLevelDeletion().markedForDeleteAt() > Long.MIN_VALUE;
    }

    /**
     * Checks if the row inside the partition was deleted
     *
     * @param row
     * @return boolean
     */
    private boolean rowIsDeleted(Row row) {
        return row.deletion().time().markedForDeleteAt() > Long.MIN_VALUE;
    }
}

