package com.rox.storm.grouping.ensure;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 *  确保消费:
 * 任何一步介绍消息失败调用 ack(tuple),或者 fail(tuple), Spout 端的 ack() 和 fail() 就会收到回调, 此时可以继续调用 collector.emit(msg, msgId), 重新发送消息
 * Spout 端持有2个 map 队列, 一个 messages<Long,String>, 用来存放每次 put 出去的队列, Long是时间戳, String 是 msg, 一个是 failMessages<Long,Integer>,  用来存放失败消息的次数, Long 是代表msgId 的 ts, Integer 记录失败的次数
 */
public class WordCountSpout implements IRichSpout{
    private TopologyContext context ;
    private SpoutOutputCollector collector ;

    private List<String> states ;

    private Random r = new Random();

    private int index = 0;

    //消息集合, 存放所有消息
    private Map<Long,String> messages = new HashMap<Long, String>();

    //失败消息
    private Map<Long,Integer> failMessages = new HashMap<Long, Integer>();

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context ;
        this.collector = collector ;
        states = new ArrayList<String>();
        states.add("hello world tom");
        states.add("hello world tomas");
        states.add("hello world tomasLee");
        states.add("hello world tomson");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if(index < 3){
            String line = states.get(r.nextInt(4));
            //取出时间戳
            long ts = System.currentTimeMillis() ;
            messages.put(ts,line);

            //发送元组，使用ts作为消息id
            collector.emit(new Values(line),ts);
            System.out.println(this + "nextTuple() : " + line + " : " + ts);
            index ++ ;
        }
    }

    /**
     * 回调处理
     */
    public void ack(Object msgId) {
        //成功处理，删除失败重试.
        Long ts = (Long)msgId ;
        failMessages.remove(ts) ;
        messages.remove(ts) ;
    }

    public void fail(Object msgId) {
        //时间戳作为msgId
        Long ts = (Long)msgId;
        //判断消息是否重试了3次
        Integer retryCount = failMessages.get(ts);
        retryCount = (retryCount == null ? 0 : retryCount) ;

        //超过最大重试次数
        if(retryCount >= 3){
            failMessages.remove(ts) ;
            messages.remove(ts) ;
        }
        else{
            //重试
            collector.emit(new Values(messages.get(ts)),ts);
            System.out.println(this + "fail() : " + messages.get(ts) + " : " + ts);
            retryCount ++ ;
            failMessages.put(ts,retryCount);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
