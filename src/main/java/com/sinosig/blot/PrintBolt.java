package com.sinosig.blot;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author suyu
 * @version 1.0.0
 * @ClassName PrintBolt
 * @Description ③第二个处理blot节点,打印blot，第三个写的类
 * @Date 2018/1/12 18:12
 */
public class PrintBolt extends BaseBasicBolt {


    private int indexId;

    /**
     * 打印blot
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String printBlot = tuple.getString(0);
        System.err.println(String.format("Bolt[%d] String recieved: %s",this.getIndexId(), printBlot));
//        System.out.println("接收到的【打印blot】:" + printBlot);
    }

    /**
     * 不做处理了
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public int getIndexId() {
        this.indexId = (int) Thread.currentThread().getId();
        return indexId;
    }
}
