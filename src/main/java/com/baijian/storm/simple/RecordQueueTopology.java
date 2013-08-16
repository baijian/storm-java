package com.baijian.storm.simple;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.baijian.storm.simple.bolt.HelloWorldBolt;
import com.baijian.storm.simple.bolt.PersistentDataBolt;
import com.baijian.storm.simple.spout.HelloWorldSpout;
import com.baijian.storm.simple.spout.ZmqSpout;

/**
 * Author: bj
 * Time: 2013-08-15 3:46 PM
 * Desc:
 */
public class RecordQueueTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //KestrelThriftSpout spout = new KestrelThriftSpout("localhost", 7000, "test", new StringScheme());
        builder.setSpout("HE", new ZmqSpout(), 10);
        builder.setBolt("record-bolt", new PersistentDataBolt(), 20).shuffleGrouping("HE");

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(16);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("record", config, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("record");
            cluster.shutdown();
        }
    }
}
