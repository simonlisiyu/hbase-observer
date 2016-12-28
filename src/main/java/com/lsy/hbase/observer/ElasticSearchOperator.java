package com.lsy.hbase.observer;


import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.ImmutableSettings;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticSearchOperator {

    // 缓冲池容量
//    private static final int MAX_BULK_COUNT = 2;
    private static final int MAX_BULK_COUNT = 1000;
    // 最大提交间隔（秒）
    private static final int MAX_COMMIT_INTERVAL = 60 * 2;

    private static Client client = null;
    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static Lock commitLock = new ReentrantLock();

    // The DateTime field name
    private static final String AIRPURIFIER_FIELD_NAME_DATETIME = "datetime";
    // The Envirment field name
    private static final String AIRPURIFIER_FIELD_NAME_ENVIRMENT = "env";
    // The IDC field name
    private static final String AIRPURIFIER_FIELD_NAME_IDC = "idc";
    // The Application field name
    private static final String AIRPURIFIER_FIELD_NAME_APPLICATION = "app";
    // The Service field name
    private static final String AIRPURIFIER_FIELD_NAME_SERVICE = "service";
    // The HostName field name
    private static final String AIRPURIFIER_FIELD_NAME_HOSTNAME = "host";

    static {

        // elasticsearch1.5.0
//        Settings settings = ImmutableSettings.settingsBuilder()
//                .put("cluster.name", Config.clusterName).build();
//        client = new TransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(
//                        Config.nodeHost, Config.nodePort));

        // 2.3.5
        client = MyTransportClient.client;

        bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.setRefresh(true);

        Timer timer = new Timer();
        timer.schedule(new CommitTimer(), 10 * 1000, MAX_COMMIT_INTERVAL * 1000);
    }

    /**
     * 判断缓存池是否已满，批量提交
     *
     * @param threshold
     */
    private static void bulkRequest(int threshold) {
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
            if (!bulkResponse.hasFailures()) {
                bulkRequestBuilder = client.prepareBulk();
            }
        }
    }

    /**
     * 加入索引请求到缓冲池
     *
     * @param builder
     */
    public static void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 加入删除请求到缓冲池
     *
     * @param builder
     */
    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 定时任务，避免RegionServer迟迟无数据更新，导致ElasticSearch没有与HBase同步
     */
    static class CommitTimer extends TimerTask {
        @Override
        public void run() {
            commitLock.lock();
            try {
                bulkRequest(0);
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                commitLock.unlock();
            }
        }
    }


    /**
     * 创建mapping(feid("indexAnalyzer","ik")该字段分词IK索引 ；feid("searchAnalyzer","ik")该字段分词ik查询；具体分词插件请看IK分词插件说明)
     * @param index 索引名称；
     * @param mappingType 索引类型
     * @param fieldSet 列集合
     * @throws Exception
     */
    public static void createMapping(String index,String mappingType,Set<String> fieldSet)throws Exception{
        // 判断index是否存在,不存在则创建索引,并启用ik分词器
        if(client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists()){
            System.out.println("index: '"+index+"' already exist!");
            new XContentFactory();
            XContentBuilder builder=XContentFactory.jsonBuilder()
                    .startObject()//注意不要加index和type startObject()_1
                    .startObject("properties") //startObject()_("properties")
                    ;

            builder = createType(builder, fieldSet);
            builder = builder.endObject(); // endObject()_("properties")
            builder = builder.endObject();   // endObject()_1

            PutMappingRequest mapping = Requests.putMappingRequest(index).type(mappingType).source(builder);
            client.admin().indices().putMapping(mapping).actionGet();
        }
        else {
            System.out.println("index not exist, create index: '"+index+"'!");
            new XContentFactory();
            XContentBuilder builder=XContentFactory.jsonBuilder()
                    .startObject()//注意不要加index和type
                    .startObject("_ttl")    // startObject("_ttl")
                    .field("enabled",true)  // 启用索引过期设置
                    .field("default","0") // 默认过期时间
                    .endObject()    // endObject()_("_ttl")
                    .startObject("properties")
                    .startObject("id").field("type", "string").field("store", "yes").endObject();
            builder = createType(builder, fieldSet);
            builder = builder.endObject().endObject();

            client.admin().indices().prepareCreate(index).addMapping(mappingType, builder).get();
        }
    }

    static XContentBuilder createType(XContentBuilder builder, Set<String> fieldSet) throws IOException {
        for(String field : fieldSet){
            if (field.equals(AIRPURIFIER_FIELD_NAME_DATETIME)) {
                System.out.println("date field: '"+field+"'!");
                builder = builder.startObject(field).field("type", "date").endObject();
            } else if (field.equals(AIRPURIFIER_FIELD_NAME_ENVIRMENT) ||
                    field.equals(AIRPURIFIER_FIELD_NAME_IDC) ||
                    field.equals(AIRPURIFIER_FIELD_NAME_APPLICATION) ||
                    field.equals(AIRPURIFIER_FIELD_NAME_SERVICE) ||
                    field.equals(AIRPURIFIER_FIELD_NAME_HOSTNAME)) {
                builder = builder.startObject(field).field("type", "string").field("analyzer", "keyword").endObject();
            } else {
                builder = builder.startObject(field).field("type", "string").field("analyzer", "english").endObject();
            }
        }
        return builder;
    }

    private static void test() {
        Config.indexName = "flume-2016-08-10";
        Config.typeName = "tweet";
        for (int i = 10; i < 20; i++) {
            Map<String, Object> json = new HashMap<String, Object>();
            json.put("field", "ttt");
            //添加
//            addUpdateBuilderToBulk(client.prepareUpdate(Config.indexName, Config.typeName, String.valueOf(i)).setDoc(json).setUpsert(json));
            //删除
            addDeleteBuilderToBulk(client.prepareDelete(Config.indexName, Config.typeName, String.valueOf(i)));
        }

        System.out.println(bulkRequestBuilder.numberOfActions());
    }

    public static void main(String[] args) {
        test();
    }
}
