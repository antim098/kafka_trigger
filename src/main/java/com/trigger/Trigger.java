package com.trigger;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Trigger implements ITrigger {

    private String topic;
    private Producer<String, String> producer;
    private ThreadPoolExecutor threadPoolExecutor;
    private AdminClient client;

    /**
     *
     */
    public Trigger() {
        Thread.currentThread().setContextClassLoader(null);
        topic = "trigger";
        producer = new KafkaProducer<String, String>(getProps());
        AdminClient client = AdminClient.create(getProps());
        threadPoolExecutor = new ThreadPoolExecutor(1, 5, 30,
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
     * @param partition
     */
    private void handleUpdate(Partition partition) {
        String key = getKey(partition);
        JSONObject obj = new JSONObject();
        obj.put("key", key);
        if (partitionIsDeleted(partition)) {
            obj.put("partitionDeleted", true);
        } else {
            UnfilteredRowIterator it = partition.unfilteredIterator();
            List<JSONObject> rows = new ArrayList<>();
            while (it.hasNext()) {
                Unfiltered un = it.next();
                if (un.isRow()) {
                    JSONObject jsonRow = new JSONObject();
                    Clustering clustering = (Clustering) un.clustering();
                    String clusteringKey = clustering.toCQLString(partition.metadata());
                    jsonRow.put("clusteringKey", clusteringKey);
                    Row row = partition.getRow(clustering);

                    if (rowIsDeleted(row)) {
                        obj.put("rowDeleted", true);
                    } else {
                        Iterator<Cell> cells = row.cells().iterator();
                        Iterator<ColumnDefinition> columns = row.columns().iterator();
                        List<JSONObject> cellObjects = new ArrayList<>();
                        while (cells.hasNext() && columns.hasNext()) {
                            JSONObject jsonCell = new JSONObject();
                            ColumnDefinition columnDef = columns.next();
                            Cell cell = cells.next();
                            jsonCell.put(columnDef.name.toString(), columnDef.type.getString(cell.value()));
                            if (cell.isTombstone()) {
                                jsonCell.put("deleted", true);
                            }
                            cellObjects.add(jsonCell);
                        }
                        jsonRow.put("cells", cellObjects);
                    }
                    rows.add(jsonRow);
                } else if (un.isRangeTombstoneMarker()) {
                    obj.put("rowRangeDeleted", true);
                    ClusteringBound bound = (ClusteringBound) un.clustering();
                    List<JSONObject> bounds = new ArrayList<>();
                    for (int i = 0; i < bound.size(); i++) {
                        String clusteringBound = partition.metadata().comparator.subtype(i).getString(bound.get(i));
                        JSONObject boundObject = new JSONObject();
                        boundObject.put("clusteringKey", clusteringBound);
                        if (i == bound.size() - 1) {
                            if (bound.kind().isStart()) {
                                boundObject.put("inclusive",
                                        bound.kind() == ClusteringPrefix.Kind.INCL_START_BOUND);
                            }
                            if (bound.kind().isEnd()) {
                                boundObject.put("inclusive",
                                        bound.kind() == ClusteringPrefix.Kind.INCL_END_BOUND);
                            }
                        }
                        bounds.add(boundObject);
                    }
                    obj.put((bound.kind().isStart() ? "start" : "end"), bounds);
                }
            }
            obj.put("rows", rows);
        }
        String value = obj.toJSONString();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            client.listTopics(new ListTopicsOptions().timeoutMs(3000)).listings().get();
            producer.send(record);
        } catch (ExecutionException | InterruptedException ex) {
            System.out.println("Kafka is not available, timed out after 3000 ms");
        }
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

    /**
     * @return
     */
    private Properties getProps() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.26.32.31:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

}
