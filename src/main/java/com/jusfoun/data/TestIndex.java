package com.jusfoun.data;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by lisiyu on 16/9/9.
 */
public class TestIndex {
    public static void main(String[] args) throws IOException {
        // on startup
        Client client = MyTransportClient.client;

        // index
        IndexResponse response = client.prepareIndex("twitter", "tweet", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
                .get();

//        String json = "{" +
//                "\"user\":\"kimchy\"," +
//                "\"postDate\":\"2013-01-30\"," +
//                "\"message\":\"trying out Elasticsearch\"" +
//                "}";
//        IndexResponse response = client.prepareIndex("twitter", "tweet")
//                .setSource(json)
//                .get();





        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        // isCreated() is true if the document is a new one, false if it has been updated
        boolean created = response.isCreated();
        System.out.println("_index="+_index);
        System.out.println("_type="+_type);
        System.out.println("_id="+_id);
        System.out.println("_version="+_version);
        System.out.println("created="+created);

        // on shutdown
        client.close();
    }
}
