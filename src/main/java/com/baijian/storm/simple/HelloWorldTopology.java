package com.baijian.storm.simple;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.baijian.storm.simple.bolt.HelloWorldBolt;
import com.baijian.storm.simple.spout.HelloWorldSpout;

/**
 * Author: bj
 * Time: 2013-08-13 1:19 PM
 * Desc: Just send Hello and append World to the end of it, at last write to file.
 */
public class HelloWorldTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Hello", new HelloWorldSpout(), 12);//设置并行度
        builder.setBolt("World", new HelloWorldBolt(), 12).shuffleGrouping("Hello");
        builder.setBolt("WorldTwo", new HelloWorldBolt(), 12).shuffleGrouping("World");

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0) {
            //设置工作进程
            config.setNumWorkers(6);//每个工作进程分配2个线程处理2个task
            config.setNumAckers(6);
            config.setMaxSpoutPending(100);
            config.setMessageTimeoutSecs(20);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Hello-World-BaiJian", config, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("Hello-World-BaiJian");
            cluster.shutdown();
        }
    }
}
