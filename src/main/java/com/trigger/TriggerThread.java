package com.trigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
        //List<ColumnDefinition> clusteringColumns = partition.metadata().clusteringColumns();
        ByteBuffer partitionKey = partition.partitionKey().getKey();
        String key = getKey(partition);  //Sample key format -- 20201205:domino_entity_name:reason
        //Using the overloaded split method because it preserves the length of the string when converting to []
        //String[] partitionValues = key.split(":", -1);
        ObjectNode partitionColsJson = MAPPER.createObjectNode();
        //Flattening all the partition Columns and creating JSON
//        for (int i = 0; i < partitionValues.length; i++) {
//            partitionColsJson.put(partitionColumns.get(i).toString(), partitionValues[i]);
//        }
        if (partitionColumns.size() == 1) {
            partitionColsJson.put(partitionColumns.get(0).name.toString(),
                    getStringValue(partitionColumns.get(0).type, partitionKey));
        } else {
            for (int i = 0; i < partitionColumns.size(); i++) {
                String columnName = partitionColumns.get(i).name.toString();
                String value = getStringValue(partitionColumns.get(i).type, CompositeType.extractComponent(partitionKey, i));
                if (value != null) {
                    partitionColsJson.put(columnName, value);
                }
            }
        }
        List<ObjectNode> rows = new ArrayList<>();
        UnfilteredRowIterator it = partition.unfilteredIterator();
        //JSONObject payload = new JSONObject();
        while (it.hasNext()) {
            Unfiltered un = it.next();
            if (un.isRow()) {
                ObjectNode payload = MAPPER.createObjectNode();
                ObjectNode jsonRow = MAPPER.createObjectNode();
                Clustering clustering = (Clustering) un.clustering();
                //String clusteringKey = clustering.toCQLString(partition.metadata());
                //Sample clusteringKey format -- key1, key2
                //String[] clusteringKeys = clusteringKey.split(", ", -1);
                //Flattening all the clustering Columns and adding to JSON row object
//                for (int i = 0; i < clusteringColumns.size(); i++) {
//                    jsonRow.put(clusteringColumns.get(i).toString(), clusteringKeys[i]);
//                }
                for (int i = 0; i < clustering.size(); i++) {
                    String columnName = partition.metadata().clusteringColumns().get(i).name.toCQLString();
                    jsonRow.put(columnName, getStringValue(partition.metadata().clusteringColumns().get(i).type,
                            clustering.get(i)));
                }
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
                            //removed trim from line 84 as well
                            //String value = columnDef.type.getString(cell.value());
                            String value = getStringValue(columnDef.cellValueType(), cell.value());
                            if (!value.equals("NULL") && !value.equals("")) {
                                jsonRow.put(columnDef.name.toString(), value);
                            }
                        }
                        jsonRow.put("table", tableName);
                        jsonRow.putAll(partitionColsJson);
                    }
                    payload.put("payload", jsonRow);
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

    private String getStringValue(AbstractType cellValueType, ByteBuffer valueBuffer) {
        Object value = null;
        if (cellValueType.toString().contains("Timestamp")) {
            try {
                value = TimestampType.instance.compose(valueBuffer).getTime();
            } catch (Exception ex) {
                value = "";
            }
        } else {
            value = cellValueType.compose(valueBuffer);
        }
        return String.valueOf(value);
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

