package com.baijian.storm.simple;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.scheme.StringScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.baijian.storm.simple.bolt.MysqlPersistenceBolt;
import com.baijian.storm.simple.spout.RabbitMQSpout;

/**
 * Author: bj
 * Time: 2013-08-20 6:18 PM
 * Desc:
 */
public class LogSaveTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("LogSource", new RabbitMQSpout("localhost",5672, true, new StringScheme()));
        builder.setBolt("SaveLog", new MysqlPersistenceBolt(new StringScheme())).shuffleGrouping("LogSource");
        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Test", config, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("Test");
            cluster.shutdown();
        }
    }
}
