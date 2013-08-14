package com.baijian.storm.simple;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.baijian.storm.simple.bolt.HelloWorldBolt;
import com.baijian.storm.simple.spout.HelloWorldSpout;

/**
 * Author: bj
 * Time: 2013-08-13 1:19 PM
 * Desc:
 */
public class HelloWorldTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Hello", new HelloWorldSpout(), 2);
        builder.setBolt("World", new HelloWorldBolt(), 2).shuffleGrouping("Hello");
        builder.setBolt("WorldTwo", new HelloWorldBolt(), 2).shuffleGrouping("World");

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
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
