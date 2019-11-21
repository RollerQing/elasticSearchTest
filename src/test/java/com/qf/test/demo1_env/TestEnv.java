package com.qf.test.demo1_env;

import org.elasticsearch.action.get.GetResponse;
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
public class TestEnv {
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

    @Test
    public void testEnv(){

        //测试1：
        System.out.println("clinet = " + client);

        System.out.println("--------------------------------->>>>>>");

        //测试2：
        //读取索引库bigdata中的type之product中的id值为1的索引信息
        GetResponse response = client.prepareGet(INDEX, TYPE, "1").get();
        System.out.println("返回的结果是：" + response);
        System.out.println("返回的source是：" + response.getSourceAsString());



    }

    @After
    public void cleanUp(){
        if(client != null){
            client.close();
        }
    }
}
