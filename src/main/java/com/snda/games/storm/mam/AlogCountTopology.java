package com.snda.games.storm.mam;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.scheme.StringScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.snda.games.storm.mam.bolt.CountBolt;
import com.snda.games.storm.mam.bolt.FilterUriBolt;
import com.snda.games.storm.mam.schema.AlogScheme;
import com.snda.games.storm.mam.spout.AlogSpout;

/**
 * Author: bj
 * Time: 2013-08-21 3:05 PM
 * Desc: Count requests of different url based on alog.
 */
public class AlogCountTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Alog", new AlogSpout("localhost", 5672, false, new StringScheme()), 10);
        builder.setBolt("FilterAlog", new FilterUriBolt(new AlogScheme()), 10).shuffleGrouping("Alog");
        builder.setBolt("GroupCount", new CountBolt(), 20).fieldsGrouping("FilterAlog", new Fields("url_id"));

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(10);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Alog-Count", config, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("Alog-Count");
            cluster.shutdown();
        }
    }
}
