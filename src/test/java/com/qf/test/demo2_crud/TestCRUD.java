package com.qf.test.demo2_crud;

import com.alibaba.fastjson.JSON;
import com.qf.test.entity.Product;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Shi shuai RollerQing on 2019/11/20 16:31
 */
public class TestCRUD {
    //TransportClient实例 用于访问远程集群
    private TransportClient client;

    private final String INDEX = "bigdata";
    private final String TYPE = "product";
    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "my-elk").build();
        client = new PreBuiltTransportClient(settings);
        //"transport_address": "192.168.37.113:9300"
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop01"), 9300));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop02"), 9300));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop03"), 9300));

    }
    /**
     * 测试新增索引
     */
    @Test
    public void testAddIndex() throws IOException {
        //需求：向索引库bigdata中的type之product中新增一条索引信息，索引信息为：name:hiveserver2, author:周润发，version:2.3.0
        //方式1：json方式
        IndexResponse indexResponse = client.prepareIndex(INDEX, TYPE, "3")
                .setSource(JSON.toJSONString(new Product("hiveserver2", "周润发", "2.3.0")),
                        XContentType.JSON).get();
        System.out.println("返回的结果是：" + indexResponse);

        System.out.println("\n_______________________________________________\n");

        //方式2：Map方式
        Map<String, String> map  = new LinkedHashMap<>();
        map.put("name", "sqoop2");
        map.put("author", "周星星");
        map.put("version", "1.4.7");

        IndexResponse indexResponse1 = client.prepareIndex(INDEX, TYPE, "4")
                .setSource(map)
                .get();
        System.out.println("返回的结果是：" + indexResponse1);
        System.out.println("\n_______________________________________________\n");

        //方式3：XContentBuilder方式
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "flume2")
                .field("author", "周兴吃")
                .field("version", "1.8.9")
                .endObject();
        IndexResponse indexResponse2 = client.prepareIndex(INDEX, TYPE, "5")
                .setSource(xContentBuilder)
                .get();
        System.out.println("返回的结果是：" + indexResponse2);
//        System.out.println("返回的结果是：" + ("created".equalsIgnoreCase(indexResponse2.getResult().toString()) ? "成功" : "失败"));

        System.out.println("\n_______________________________________________\n");

        //方式4：可变长参数方式

        IndexResponse indexResponse3 = client.prepareIndex(INDEX, TYPE, "5")
                .setSource("name", "kafka", "author", "张曼玉", "version", "1.2.5")
                .get();
//        System.out.println("返回的结果是：" + ("created".equalsIgnoreCase(indexResponse3.getResult().toString()) ? "成功" : "失败"));
        System.out.println("返回的结果是：" + indexResponse3);


    }
//
//    @Test
//    public void testDelete(){
//        //需求：删除索引库bigdata中的type类型之product中id为4
//        DeleteResponse deleteResponse = client.prepareDelete(INDEX, TYPE, "5").get();
//        System.out.println("返回的结果是：" + deleteResponse);
//
//    }
//    //测试更新索引（局部）
//    @Test
//    public void testUpdate(){
//        //需求：更新索引库bigdata中的type类型之product中id为5，将author更新为泰森
//        UpdateResponse updateResponse = client.prepareUpdate(INDEX, TYPE, "5")
//                .setDoc("author", "泰森")
//                .get();
//        System.out.println("返回的结果是：" + updateResponse);
//    }


    @After
    public void cleanUp(){
        if(client != null){
            client.close();
        }
    }
}
