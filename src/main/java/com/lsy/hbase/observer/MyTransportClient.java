package com.lsy.hbase.observer;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by lisiyu on 16/9/9.
 */
public class MyTransportClient {
    public static Settings settings;
    public static Client client;

    static {
        settings = Settings.settingsBuilder()
                .put("cluster.name", Config.clusterName).build();
        try {
            TransportAddress[] addresses = new TransportAddress[Config.nodeHost.split("||").length];
            for(int i=0; i<Config.nodeHost.split("||").length; i++){
                addresses[i] = new InetSocketTransportAddress(InetAddress.getByName(Config.nodeHost.split("||")[i]), 9300);
            }
            client = TransportClient.builder().settings(settings).build().addTransportAddresses(addresses);
//            client = TransportClient.builder().settings(settings).build()
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
