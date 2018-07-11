package com.rox.redis_jedis.conn_string_test;

import com.google.gson.Gson;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JedisClientTest {

    /**
     * 使用 Jedis 对象连接到 redis
     */
    @Test
    public void testPingJedis() {
        //创建一个 jedis 客户端对象 (redis 的客户端连接)
        Jedis client = new Jedis("cs1", 6379);

        //测试服务器是否连通
        String resp = client.ping();
        System.out.println(resp);
    }


    /**
     * 将对象序列化为 byte 数组 存入 redis
     * 再 从 redis 中取出来反序列化
     *
     * @throws Exception
     */
    @Test
    public void testObjectCache() throws Exception {
        //创建一个jedis客户端对象（redis的客户端连接）
        Jedis jedis = new Jedis("cs1", 6379);
        Student student = new Student(9527, "唐伯虎", "男", 33, "苏州府");

//  存  
        // 将对象序列化成字节数组
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);

        // 用对象序列化流来将student对象序列化，然后把序列化之后的二进制数据写到baos流中
        oos.writeObject(student);

        // 将baos流转成byte数组
        byte[] pBytes = baos.toByteArray();

        // 将对象序列化之后的byte数组存到redis的string结构数据中
        jedis.set("student_tang".getBytes(), pBytes);

//  取  
        // 根据key从redis中取出对象的byte数据
        byte[] pBytesResponse = jedis.get("student_tang".getBytes());

        // 将byte数据反序列出对象
        ByteArrayInputStream bais = new ByteArrayInputStream(pBytesResponse);
        ObjectInputStream ois = new ObjectInputStream(bais);

        // 从对象读取流中读取出p对象
        Student pResp = (Student) ois.readObject();

        System.out.println(pResp);
    }



    /**
     * 将对象转成json字符串缓存到redis的string结构数据中
     */
    @Test
    public void testObjectToJsonCache() {

        //创建一个jedis客户端对象（redis的客户端连接）
        Jedis jedis = new Jedis("cs1", 6379);

        Student student = new Student(1002, "ycy", "f", 20, "Jiangsu/yancheng");

        //  存  
        // 利用gson将对象转成json串
        Gson gson = new Gson();
        String pJson = gson.toJson(student);

        // 将json串存入redis
        jedis.set("beauty_yang", pJson);

        //  取  
        // 从redis中取出对象的json串
        String pJsonResp = jedis.get("beauty_yang");

        // 将返回的json解析成对象
        Student student_tang = gson.fromJson(pJsonResp, Student.class);

        // 显示对象的属性
        System.out.println(student_tang);
    }





}
