package com.mark.es.demo2;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

/**
 * Created by lulei on 2018/6/11.
 */
public class UpsertCarInfoApp {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Settings settings = Settings.builder()
                .put("cluster.name","my-es")
                .put("client.transport.sniff",true)
                .build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));


        XContentBuilder indexBuilder = XContentFactory.jsonBuilder().startObject()
                .field("brand","宝马")
                .field("name","bmw320")
                .field("price",3100000)
                .field("produce_date","2017-01-01")
                .endObject();

        IndexRequest indexRequest = new IndexRequest("car_shop","cars","1")
                .source(indexBuilder);

        XContentBuilder updateBuilder = XContentFactory.jsonBuilder().startObject()
                .field("price",310000)
                .endObject();

        UpdateRequest updateRequest = new UpdateRequest("car_shop","cars","1")
                .doc(updateBuilder).upsert(indexRequest);

        UpdateResponse response = client.update(updateRequest).get();
        System.out.println(response.getResult().getOp());
        client.close();

    }
}
