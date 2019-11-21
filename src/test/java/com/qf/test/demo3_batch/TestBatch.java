package com.qf.test.demo3_batch;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Shi shuai RollerQing on 2019/11/20 16:31
 */
public class TestBatch {
    //TransportClient实例 用于访问远程集群
    private TransportClient client;

    private final String INDEX = "bigdata";
    private final String TYPE = "product";
    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "my-elk").build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop01"), 9300));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop02"), 9300));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop03"), 9300));

    }

    //测试批量处理
    @Test
    public void testBatchDealWith(){
        //需求：使用批处理技术 操作es索引库值bigdata 完成下述操作： 将增删改放在一起处理了
        //①新增索引信息 name:beam, author:刘德华,version 4.4.4
        //②删除索引信息 id ： 2
        //③修改id为5的索引信息 ：  author : 施瓦辛格

        BulkResponse bulkItemResponses = client.prepareBulk()
                .add(new IndexRequest(INDEX, TYPE).source("name", "beam", "author", "刘德华", "version", "4.4.4"))
                .add(new DeleteRequest(INDEX, TYPE, "2"))
                .add(new UpdateRequest(INDEX, TYPE, "5").doc("author", "施瓦辛格"))
                .get();

        BulkItemResponse[] items = bulkItemResponses.getItems();
        for (BulkItemResponse item : items) {
            System.out.println("失败否？" + item.isFailed());
        }
    }
    @After
    public void cleanUp(){
        if(client != null){
            client.close();
        }
    }
}
