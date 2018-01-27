package com.sinosig.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * @author suyu
 * @version 1.0.0
 * @ClassName RandomSentenceSpout
 * @Description ① 第一个写的是Spout
 *              spout，数据源的源头，会将【"谁":"说了什么"】这样的格式发射到blot中
 *              open方法主要由storm框架传入SpoutOutputCollector
 *              nextTuple方法主要是发射数据源
 * @Date 2018/1/12 17:22
 */
public class RandomSentenceSpout extends BaseRichSpout {
    /**
     * 用来收集Spout输出的tuple
     */
    private SpoutOutputCollector collector;
    private Random random;
    private static String[] sentences = new String[] {"edi:I'm happy",
            "marry:I'm angry", "john:I'm sad", "ted:I'm excited", "laden:I'm dangerous" ,"suyu:I'm Hello World"};


    /**
     * 该方法调用一次，主要由storm框架传入SpoutOutputCollector
     * @param conf 具体不详
     * @param context 拓扑上下文
     * @param collector 数据源的收集器
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    /**
     * 此方法是从ISpout接口实现的。
     * 当此方法被调用时，storm会主动请求soupt发射元组给输出的collector，这个方法应该是非阻塞的，
     * 所以如果spout没有元组发射，这个方法应该进行返回。
     * When this method is called, Storm is requesting that the Spout emit tuples to the
     * output collector. This method should be non-blocking, so if the Spout has no tuples
     * to emit, this method should return. nextTuple, ack, and fail are all called in a tight
     * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous
     * to have nextTuple sleep for a short amount of time (like a single millisecond)
     * so as not to waste too much CPU.
     */
    @Override
    public void nextTuple() {
        //从自定义数组中随机取出每次说话的句子
        String toSay = sentences[random.nextInt(sentences.length)];
        //将其发射出去，这里的values是继承了ArrayList
        this.collector.emit(new Values(toSay));
    }

    /**
     * 该方法用来给我们发射的value在整个Stream中定义一个别名。可以理解为key。该值必须在整个topology定义中唯一。
     * 声明输出视图给此拓扑的所有流
     * Declare the output schema for all the streams of this topology.
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
