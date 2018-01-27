package com.sinosig.blot;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @ClassName AddSomeThingBolt
 * @Description ②第二个写的类是处理元数据的blot类
 * @author suyu
 * @Date 2018/1/12 18:05
 * @version 1.0.0
 */
public class AddSomeThingBolt extends BaseBasicBolt {

    private int indexId;

    /**
     * blot的执行方法，实现了IBasicBolt接口，
     * 进程输入元组，选择性的提交新的元组基于输入的元组（个人翻译）
     * Process the input tuple and optionally emit new tuples based on the input tuple.
     * All acking is managed for you. Throw a FailedException if you want to fail the tuple.
     * @param tuple  从spout中发射过来的句子，个人理解
     * @param collector 类似spout 中的收集器，也是起到发射的作用
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //拿到tuple中的第一个元素
        String sentence = (String) tuple.getValue(0);
        //进行逻辑拼接
        String out = sentence + "!!!! yoyoyo!!~~";
        //继续将其进行发射给下一个blot
        collector.emit(new Values(out));
        System.err.println(String.format("--------------AddSomeThingBolt[%d] ------------------",this.getIndexId()));
    }

    /**
     * 类似spout中的方法，该方法用来给我们发射的value在整个Stream中定义一个别名。可以理解为key。该值必须在整个topology定义中唯一。
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("add_sentence"));
    }


    public int getIndexId() {
        this.indexId = (int) Thread.currentThread().getId();
        return indexId;
    }
}
