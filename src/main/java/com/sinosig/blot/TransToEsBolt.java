package com.sinosig.blot;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
/**
 * @ClassName TransToEsBolt
 * @Description (这里用一句话描述这个类的作用)
 * @author suyu
 * @Date 2018/1/14 11:12
 * @version 1.0.0
 */
public class TransToEsBolt extends BaseBasicBolt {


    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
