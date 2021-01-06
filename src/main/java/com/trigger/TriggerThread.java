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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.zip.GZIPOutputStream;

public class TriggerThread implements Callable<Object> {
    private static Logger logger = LoggerFactory.getLogger(TriggerThread.class);
    private Partition partition;
    //private Producer<String, String> producer;
    private Producer<String, byte[]> producer;
    private String topic;
    //private Properties properties = new Properties();

    public TriggerThread(Producer<String, byte[]> producer, Partition partition, String topic) {
        this.producer = producer;
        this.partition = partition;
        this.topic = topic;
    }

    @Override
    public Object call() {
        if (partitionIsDeleted(partition)) {
            return null;
        }
        ArrayList<String> colNames = new ArrayList<>(Arrays.asList("create_uid", "create_dts", "item_short_name", "jobid", "update_uid", "update_dts"));
        String tableName = partition.metadata().cfName;
        List<ColumnDefinition> partitionColumns = partition.metadata().partitionKeyColumns();
        List<ColumnDefinition> clusteringColumns = partition.metadata().clusteringColumns();
        String key = getKey(partition);
        String[] partitionValues = key.split(":");
        JSONObject partitionColsJson = new JSONObject();
        //Flattening all the partition Columns and creating JSON
        for (int i = 0; i < partitionColumns.size(); i++) {
            partitionColsJson.put(partitionColumns.get(i).toString(), partitionValues[i]);
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
                String[] clusteringKeys = clusteringKey.split(",");
                //Flattening all the clustering Columns and adding to JSON row object
                for (int i = 0; i < clusteringColumns.size(); i++) {
                    jsonRow.put(clusteringColumns.get(i).toString(), clusteringKeys[i]);
                }
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
                            if (colNames.contains(columnDef.name.toString())) {
                                jsonRow.put(columnDef.name.toString(), columnDef.type.getString(cell.value()));
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
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        byte[] row = new byte[0];
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(rows.toString().getBytes());
            gzip.close();
            rows.clear();
            row = out.toByteArray();
        } catch (IOException e) {
            logger.info("Error in compression");
        }

        //String value = rows.toString();
        //ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        try {
            if (Trigger.getKafkaStatus()) {
                producer.send(new ProducerRecord<>(topic, key, row));
                //fileWriter.write("\n" + value);
                //producer.send(new ProducerRecord<String, String>(topic, key, "[{\"payload\":{\"raw_ts\":\"87c5402b-2e4e-11eb-907d-8bc5adaa2362\",\"fallout_name\":\"domino_deleted_chassis_module_raw\",\"reason\":\"Invalid DELETED_DTS\",\"loadtime\":\"2020-12-21 23:19:37.006000+0000\",\"record_info_map\":\"{'chassis_id': '14252', 'deleted_dts': '2020-09-30 15:09:45.663', 'module_id': '29'}\",\"table\":\"etl_fallout_trigger\",\"ds\":\"20200930\"}}]"));
                //producer.send(new ProducerRecord<String, String>(topic, key,"[{\"payload\":{\"create_dts\":\"2017-07-27 12:12:00.457\",\"commodity_code\":\"10000000\",\"sku_requestor_uid\":\"NULL\",\"orderable\":\"1\",\"alt_product_group1_code\":\"NULL\",\"item_class_id\":\"NA000\",\"clave_prod_serv\":\"NULL\",\"hsn_sac_code\":\"NULL\",\"update_dts\":\"2017-08-08 07:55:31.147\",\"ds\":\"20200928\",\"sku_source_code\":\"NULL\",\"item_weight\":\"0.000\",\"jobid\":\"1607602101804\",\"shippable_flag\":\"0\",\"ship_class\":\"NULL\",\"product_mfg_code\":\"NULL\",\"unspsc_code\":\"NULL\",\"gbwitem_flag\":\"0\",\"item_short_name\":\"CSTMA:FMS(FY18Q2) /PS:TS/ 18 month (61-78 M) / PowerConnect 2816\",\"item_keyword\":\"NULL\",\"manufacturer_id\":\"NULL\",\"dell_mfg_part_number\":\"NULL\",\"emc_origin\":\"0\",\"sku_id\":\"11320110\",\"create_uid\":\"ASIA-PACIFIC\\\\Unik_You\",\"line_item_override_flag\":\"0\",\"si_number\":\"NULL\",\"copy_svc_control_flag\":\"NULL\",\"exception_exists_flag\":\"0\",\"corp_discount_flag\":\"0\",\"china_commodity_code\":\"NULL\",\"line_of_business_code\":\"70\",\"end_of_life_date\":\"NULL\",\"update_uid\":\"BATCH::Unik_You\",\"flexi_sku\":\"0\",\"ready_to_order_date\":\"2017-07-27 11:50:11.000\",\"recurring_bill_flag\":\"0\",\"region\":\"GLOBAL\",\"ndaa_flag\":\"0\",\"product_group_code\":\"NULL\",\"sabrix_tax_code\":\"97520\",\"status_code\":\"D\",\"sku_num\":\"547-37152\",\"parent_qty\":\"3\",\"archived\":\"0\",\"emea_commodity_code\":\"NULL\",\"global_sku\":\"0\",\"vitem_flag\":\"0\",\"sku_owner_uid\":\"NULL\",\"pds_tested\":\"0\",\"item_type_code\":\"8\",\"revenue_code\":\"O3\",\"table\":\"domino_item_raw_wt_p1\",\"sku_type_code\":\"APOSWARR\",\"alt_product_group2_code\":\"NULL\",\"comments\":\"NULL\",\"mwd_flag\":\"0\",\"nbd_flag\":\"NO\",\"manufacturer_name\":\"NULL\",\"item_long_name\":\"CSTMA:FMS(FY18Q2) /PS:TS/ 18 month (61-78 M) / PowerConnect 2816\",\"scm_guid\":\"F80D44B2-C51A-41BA-A839-18D6469A6566\",\"material_number\":\"NULL\",\"charge_shipping_with_sys_flag\":\"0\",\"item_ownership_group_id\":\"1\",\"gedis_class_code\":\"C993\",\"ts\":\"23bbf6f3-3afd-11eb-85d9-91d4c3908712\"}}]"));
                //producer.flush();
//            } else {
//                //Sending records to file in case kafka is down.
//                //fileWriter.write("\n" + value);
//            }
            }
        } catch (Exception ex) {
            Trigger.setKafkaStatus(false);
            logger.info("===================Exception while sending record to producer==============");
            logger.info(ex.getMessage(), ex);
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

