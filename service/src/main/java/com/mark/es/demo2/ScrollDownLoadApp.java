package com.mark.es.demo2;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by lulei on 2018/6/11.
 */
public class ScrollDownLoadApp {
    public static void main(String[] args) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name","my-es")
                .put("client.transport.sniff",true)
                .build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));


        SearchResponse response = client.prepareSearch("car_shop").setTypes("sales")
                .setScroll(new TimeValue(1000))
                .setQuery(QueryBuilders.termQuery("brand.keyword", "宝马"))
                .setSize(1)
                .get();

        int batchCount = 0;
        do{
            for(SearchHit searchHit:response.getHits()){
                System.out.println("batchCount:" + ++batchCount);
                System.out.println(searchHit.getSourceAsString());
            }
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(2000))
                    .execute().actionGet();
        }while (response.getHits().getHits().length != 0);
    }
}
