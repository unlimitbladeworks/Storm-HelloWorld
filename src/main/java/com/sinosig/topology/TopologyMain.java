package com.sinosig.topology;

import com.sinosig.blot.AddSomeThingBolt;
import com.sinosig.blot.PrintBolt;
import com.sinosig.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * @ClassName TopologyMain
 * @Description ④ 第四个写的类，创建之前的所有节点连线，此类为拓扑类
 * @author suyu
 * @Date 2018/1/12 17:20
 * @version 1.0.0
 */
public class TopologyMain {

    public static void main(String[] args) throws Exception {
        //1.创建拓扑builder
        TopologyBuilder builder = new TopologyBuilder();
        //2.创建连线节点，把spout放入topology中
        builder.setSpout("spout",new RandomSentenceSpout());
        //3.创建第一个bolt节点放入topology中，shuffleGrouping，Storm按照何种策略将tuple分配到后续的bolt去。
        builder.setBolt("addBlot", new AddSomeThingBolt(),2).shuffleGrouping("spout");
        //4.创建第二个bolt节点放入topology中
        builder.setBolt("printBlot", new PrintBolt(),3).shuffleGrouping("addBlot");

        Config conf = new Config();
        conf.setDebug(false);
        //若有参数，则执行storm集群提交，若没有本地模拟集群提交topology
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
