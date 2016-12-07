package com.lsy.hbase.observer;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.*;

/**
 * Created by lisiyu on 16/9/19.
 */
public class ElasticSearchBulkProcessor {

    private static Client client = null;
    private static BulkProcessor bulkProcessor = null;

    // 缓冲池容量(计数,request)
    private static final int MAX_BULK_COUNT = 1000;
    // 缓冲池容量（大小,MB）
    private static final int MAX_BULK_SIZE = 1;
    // 最大提交间隔（秒）
    private static final int MAX_COMMIT_INTERVAL = 60 * 2;
    // 最大并发数量
    private static final int MAX_CONCURRENT_REQUEST = 2;
    // 失败重试等待时间 (ms)
    private static final int REJECT_EXCEPTION_RETRY_WAIT = 500;
    // 失败重试次数
    private static final int REJECT_EXCEPTION_RETRY_TIMES = 3;

    static {
        // 2.3.5
        client = MyTransportClient.client;

        bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {  }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {  }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {  }
                })
                .setBulkActions(MAX_BULK_COUNT)
                .setBulkSize(new ByteSizeValue(MAX_BULK_SIZE, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(MAX_COMMIT_INTERVAL))
                .setConcurrentRequests(MAX_CONCURRENT_REQUEST)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(
                                TimeValue.timeValueMillis(REJECT_EXCEPTION_RETRY_WAIT),
                                REJECT_EXCEPTION_RETRY_TIMES))
                .build();
    }

    /**
     * 加入索引请求到缓冲池
     *
     * @param indexRequest
     * @param fieldSet
     */
    public static void addIndexRequestToBulkProcessor(IndexRequest indexRequest,Set<String> fieldSet) {
        try {
            // 获取索引及类型信息
            System.out.println("index:"+indexRequest.index());
            System.out.println("type:"+indexRequest.type());

            // 尝试创建索引,并指定ik中文分词
            createMapping(indexRequest.index(),indexRequest.type(),fieldSet);

            // 更新数据
            bulkProcessor.add(indexRequest);
        } catch (Exception ex) {
            ex.printStackTrace();
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
            System.out.println("index: '"+index+"' is exist!");
            new XContentFactory();
            XContentBuilder builder=XContentFactory.jsonBuilder()
                    .startObject()//注意不要加index和type startObject()_1
//                    .startObject("_ttl")    // startObject("_ttl")
//                    .field("enabled",true)  // 启用索引过期设置
//                    .field("default","1m") // 默认过期时间
//                    .endObject()    // endObject()_("_ttl")
                    .startObject("properties") //startObject()_("properties")
//                    .startObject("id").field("type", "string").field("store", "yes").endObject()
                    ;
            for(String field : fieldSet){
                builder = builder.startObject(field).field("type", "string").field("store", "yes")
//                        .field("analyzer", "ik") // 是否启用分词, 和分词插件名称
                        .endObject();
            }
            builder = builder.endObject(); // endObject()_("properties")
            builder = builder.endObject();   // endObject()_1

            PutMappingRequest mapping = Requests.putMappingRequest(index).type(mappingType).source(builder);
            client.admin().indices().putMapping(mapping).actionGet();

//            // You can also provide the type in the source document
//            client.admin().indices().preparePutMapping("twitter")
//                    .setType("user")
//                    .setSource("{\n" +
//                            "    \"user\":{\n" +
//                            "        \"properties\": {\n" +
//                            "            \"name\": {\n" +
//                            "                \"type\": \"string\"\n" +
//                            "            }\n" +
//                            "        }\n" +
//                            "    }\n" +
//                            "}")
//                    .get();

        }
        else {
            System.out.println("create index: '"+index+"'!");
            new XContentFactory();
            XContentBuilder builder=XContentFactory.jsonBuilder()
                    .startObject()//注意不要加index和type
                    .startObject("_ttl")    // startObject("_ttl")
                    .field("enabled",true)  // 启用索引过期设置
                    .field("default","1m") // 默认过期时间
                    .endObject()    // endObject()_("_ttl")
                    .startObject("properties")
                    .startObject("id").field("type", "string").field("store", "yes").endObject();
            for(String field : fieldSet){
                builder = builder.startObject(field).field("type", "string").field("store", "yes")
//                        .field("analyzer", "ik")
                        .endObject();
            }
            builder = builder.endObject().endObject();

            client.admin().indices().prepareCreate(index).addMapping(mappingType, builder).get();
        }
    }

    public static void test() {
        // on startup
        Client client = MyTransportClient.client;
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {  }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {  }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {  }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("field", "test");
        bulkProcessor.add(new IndexRequest("twitter", "tweet", "1111").source(json));
    }

    public static void main(String[] args) {
        test();
    }
}
