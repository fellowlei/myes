package com.mark.es.demo2;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Created by lulei on 2018/6/11.
 */
public class BulkUploadSalesDataApp {
    public static void main(String[] args) throws IOException {
        Settings settings = Settings.builder()
                .put("cluster.name","my-es")
                .put("client.transport.sniff",true).build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        // add
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("car_shop", "sales", "3")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("brand", "奔驰")
                        .field("name", "奔驰C200")
                        .field("price", 350000)
                        .field("produce_date", "2017-01-05")
                        .field("sale_price", 340000)
                        .field("sale_date", "2017-02-03")
                        .endObject());
        bulkRequestBuilder.add(indexRequestBuilder);

        //update
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("car_shop", "sales", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("sales_price", "290000")
                        .endObject());
        bulkRequestBuilder.add(updateRequestBuilder);

        // del
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("car_shop", "sales", "2");
        bulkRequestBuilder.add(deleteRequestBuilder);

        BulkResponse responses = bulkRequestBuilder.get();
        for (BulkItemResponse response : responses.getItems()) {
            System.out.println(response.getVersion());
        }
        client.close();
    }
}
