package com.mark.es.demo;

import org.apache.http.impl.client.CloseableHttpClient;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class PoiInfo {

    public static TransportClient client = null;
    public static void init(){
        try {  Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
             client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    static {
        init();
    }

    public static List<String> loadFile(){
        List<String> resultList = new ArrayList<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("d:/abc.csv"), "gbk"));
            String line = null;
            int i=0;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);

                String[] arr = line.split(",");
                if(arr.length >3){
                    String result = arr[0] +","+ arr[1] +","+ arr[2];
                    resultList.add(result);
                }
                if(i > 20000){
                    break;
                }
                i++;
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }


    public static void outputPhone(List<String> list,String fileName){
        try {
            PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(fileName),"gbk"));
            for (String line : list) {
                printWriter.println(line);
            }
            printWriter.flush();
            printWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void add(List<String> list) throws IOException {

        for(int i=0; i<list.size(); i++){
            String line = list.get(i);
            System.out.println(line);
            String[] arr = line.split(",");
            String phone = arr[0];
            String xiaoqu= arr[1];
            String addr = arr[2];
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .field("phone",phone)
                    .field("xaioqu",xiaoqu)
                    .field("addr",addr)
                    .field("ctime",System.currentTimeMillis())
                    .endObject();
            IndexResponse response = client.prepareIndex("order", "person", phone).setSource(builder).get();
            System.out.println(i + ":" + response.getResult());
        }

    }

    public static void getPerson(String id){
        GetResponse response = client.prepareGet("order", "person", id).get();
        System.out.println(response.getSourceAsString());
    }



    public static void main(String[] args) throws IOException {
//        List<String> list = loadFile();
//        System.out.println(list);
//        add(list);
        getPerson("13911112222");
    }
}
