package com.snda.games.storm.mam;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.scheme.StringScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.snda.games.storm.mam.amqp.SharedQueueWithBinding;
import com.snda.games.storm.mam.bolt.logrecord.FilterLogBolt;
import com.snda.games.storm.mam.bolt.logrecord.SaveLogBolt;
import com.snda.games.storm.mam.schema.TableLogScheme;
import com.snda.games.storm.mam.spout.AMQPSpout;

import java.io.FileInputStream;
import java.util.Properties;

public class LogRecordTopology {

    public static void main(String[] args) throws Exception {

        String host = "";
        Integer port = 5672;
        String username = "";
        String password = "";
        String vhost = "";

        String dbHost = "";
        String dbPort = "";
        String dbUsername = "";
        String dbPassword = "";
        String dbName = "";

        Properties properties = new Properties();
        properties.load(new FileInputStream("logrecord.local.properties"));
        host = properties.getProperty("host");
        port = Integer.parseInt(properties.getProperty("port"));
        username = properties.getProperty("username");
        password = properties.getProperty("password");
        vhost = properties.getProperty("vhost");

        dbHost = properties.getProperty("dbhost");
        dbPort = properties.getProperty("dbport");
        dbUsername = properties.getProperty("dbusername");
        dbPassword = properties.getProperty("dbpassword");
        dbName = properties.getProperty("dbname");

        StringScheme stringScheme = new StringScheme();
        SharedQueueWithBinding sharedQ = new SharedQueueWithBinding("", "", "");

        TopologyBuilder builder = new TopologyBuilder();
        AMQPSpout amqpSpout = new AMQPSpout(stringScheme, host, port, username,
                password, vhost, sharedQ, true, false);
        FilterLogBolt filterLogBolt = new FilterLogBolt(new TableLogScheme());
        SaveLogBolt saveLogBolt = new SaveLogBolt(dbHost, dbPort, dbName, dbUsername, dbPassword, 10);
        builder.setSpout("LogRecord", amqpSpout, 10);
        builder.setBolt("FilterLog", filterLogBolt, 10).shuffleGrouping("LogRecord");
        builder.setBolt("SaveLog", saveLogBolt, 10).shuffleGrouping("FilterLog");

        Config config = new Config();

        if (args != null && args.length > 0) {
            config.setNumWorkers(20);
            config.setMaxSpoutPending(1000);
            config.setNumAckers(5);
            config.setMessageTimeoutSecs(30);//default
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("RecordLog", config, builder.createTopology());
            Utils.sleep(10000);
            localCluster.killTopology("RecordLog");
            localCluster.shutdown();
        }
    }
}
