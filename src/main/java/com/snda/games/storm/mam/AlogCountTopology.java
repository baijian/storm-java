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

import java.io.FileInputStream;
import java.util.Properties;

public class AlogCountTopology {

    public static void main(String[] args) throws Exception {
        String spoutHost = "";
        Integer spoutPort = 5672; //default
        String dbHost = "";
        String dbPort = "";
        String dbName = "";
        String username = "";
        String password = "";
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
            spoutHost = properties.getProperty("spouthost");
            spoutPort = Integer.parseInt(properties.getProperty("spoutport"));
            dbHost = properties.getProperty("dbhost");
            dbPort = properties.getProperty("dbport");
            dbName = properties.getProperty("dbname");
            username = properties.getProperty("username");
            password = properties.getProperty("password");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Alog", new AlogSpout(spoutHost, spoutPort, false, new StringScheme()), 10);

        builder.setBolt("FilterAlog", new FilterUriBolt(new AlogScheme(), dbHost, dbPort, dbName
                , username, password), 10).shuffleGrouping("Alog");

        builder.setBolt("GroupCount", new CountBolt(dbHost, dbPort, dbName, username, password), 20)
                .fieldsGrouping("FilterAlog", new Fields("url_id"));

        Config config = new Config();
//        config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(10);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Alog-Count", config, builder.createTopology());
            Utils.sleep(240000);
            cluster.killTopology("Alog-Count");
            cluster.shutdown();
        }
    }
}
