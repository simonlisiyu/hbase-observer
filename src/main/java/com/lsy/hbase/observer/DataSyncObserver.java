package com.lsy.hbase.observer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.*;

//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.ImmutableSettings;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import java.util.NavigableMap;

public class DataSyncObserver extends BaseRegionObserver {

    private static Client client = null;
    private static final Log LOG = LogFactory.getLog(DataSyncObserver.class);


    /**
     * 读取HBase Shell的指令参数
     *
     * @param env
     */
    private void readConfiguration(CoprocessorEnvironment env) {
        Configuration conf = env.getConfiguration();
        Config.clusterName = conf.get("es_cluster");
        Config.nodeHost = conf.get("es_host");
        Config.nodePort = conf.getInt("es_port", -1);
        Config.indexName = conf.get("es_index");
        Config.typeName = conf.get("es_type");

        LOG.info("observer -- started with config: " + Config.getInfo());
    }


    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        readConfiguration(env);
//        Settings settings = ImmutableSettings.settingsBuilder()
//                .put("cluster.name", Config.clusterName).build();
//        client = new TransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(
//                        Config.nodeHost, Config.nodePort));
        client = MyTransportClient.client;
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append, Result result) throws IOException {
        try {
            LOG.info("observer -- append new doc: " + append.getRow() + " to type: " + Config.typeName);
            String indexId = new String(append.getRow());
            NavigableMap familyMap = append.getFamilyCellMap();
            HashSet set = new HashSet();
            HashMap json = new HashMap();
            Iterator mapIterator = familyMap.entrySet().iterator();

            while(mapIterator.hasNext()) {
                Map.Entry entry = (Map.Entry)mapIterator.next();
                Iterator valueIterator = ((List)entry.getValue()).iterator();

                while(valueIterator.hasNext()) {
                    Cell cell = (Cell)valueIterator.next();
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    json.put(key, value);
                    set.add(key);
                }
            }

            ElasticSearchBulkProcessor.addIndexRequestToBulkProcessor((new UpdateRequest(Config.indexName, Config.typeName, indexId)).doc(json), set);
            LOG.info("observer -- add new doc: " + indexId + " to type: " + Config.typeName);
        } catch (Exception ex) {
            LOG.error(ex);
        }
        return result;
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        /**
         * 原方法调用ElasticSearchOperator,没有通过IK创建中文索引。
         */
//        try {
//            String indexId = new String(put.getRow());
//            Map<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
////            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
//            Map<String, Object> json = new HashMap<String, Object>();
//            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
//                for (Cell cell : entry.getValue()) {
//                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
//                    String value = Bytes.toString(CellUtil.cloneValue(cell));
//                    json.put(key, value);
//                }
//            }
////            System.out.println("postPut");
//            ElasticSearchOperator.addUpdateBuilderToBulk(client.prepareUpdate(Config.indexName, Config.typeName, indexId).setDoc(json).setUpsert(json));
//            LOG.info("observer -- add new doc: " + indexId + " to type: " + Config.typeName);
//        } catch (Exception ex) {
//            LOG.error(ex);
//        }

        /**
         * 新方法调用ElasticSearchBulkProcessor,通过IK创建中文索引。
         */
        try {
            String indexId = new String(put.getRow());
            NavigableMap familyMap = put.getFamilyCellMap();
            HashSet set = new HashSet();
            HashMap json = new HashMap();
            Iterator mapIterator = familyMap.entrySet().iterator();

            while(mapIterator.hasNext()) {
                Map.Entry entry = (Map.Entry)mapIterator.next();
                Iterator valueIterator = ((List)entry.getValue()).iterator();

                while(valueIterator.hasNext()) {
                    Cell cell = (Cell)valueIterator.next();
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    json.put(key, value);
                    set.add(key);
                }
            }

            System.out.println();
//            ElasticSearchBulkProcessor.addIndexRequestToBulkProcessor((new IndexRequest(Config.indexName, Config.typeName, indexId)).source(json), set);
            ElasticSearchBulkProcessor.addIndexRequestToBulkProcessor((new UpdateRequest(Config.indexName, Config.typeName, indexId)).doc(json), set);
            LOG.info("observer -- add new doc: " + indexId + " to type: " + Config.typeName);
        } catch (Exception ex) {
            LOG.error(ex);
        }
    }

    @Override
    public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> e, final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
        try {
            String indexId = new String(delete.getRow());
            ElasticSearchOperator.addDeleteBuilderToBulk(client.prepareDelete(Config.indexName, Config.typeName, indexId));
            LOG.info("observer -- delete a doc: " + indexId);
        } catch (Exception ex) {
            LOG.error(ex);
        }
    }

    private static void testGetPutData(String rowKey, String columnFamily, String column, String value) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        Map<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
//        NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
        System.out.println(Bytes.toString(put.getRow()));
        for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
            Cell cell = entry.getValue().get(0);
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void main(String[] args) {
        testGetPutData("111", "cf", "c1", "hello world");
    }
}
