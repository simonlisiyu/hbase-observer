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

    private static final String SPLIT_WORD = "--";

    static {
        settings = Settings.settingsBuilder()
                .put("cluster.name", Config.clusterName).build();
        try {
            TransportAddress[] addresses = new TransportAddress[Config.nodeHost.split(SPLIT_WORD).length];
            for(int i=0; i<Config.nodeHost.split(SPLIT_WORD).length; i++){
                System.out.println("Config.nodeHost="+Config.nodeHost+", i="+i);
                addresses[i] = new InetSocketTransportAddress(InetAddress.getByName(Config.nodeHost.split(SPLIT_WORD)[i]), 9300);
            }
            client = TransportClient.builder().settings(settings).build().addTransportAddresses(addresses);

//            System.out.println("Config.nodeHost="+Config.nodeHost);
//            client = TransportClient.builder().settings(settings).build()
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Config.nodeHost), 9300));

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
