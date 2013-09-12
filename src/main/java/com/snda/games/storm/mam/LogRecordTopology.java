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
        String queueName = "";
        String exchange = "";
        String routinKey = "";

        String dbHost = "";
        String dbPort = "";
        String dbUsername = "";
        String dbPassword = "";
        String dbName = "";

        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/resources/recordlog-local.properties"));
        host = properties.getProperty("amqpHost");
        port = Integer.parseInt(properties.getProperty("amqpPort"));
        username = properties.getProperty("amqpUsername");
        password = properties.getProperty("amqpPassword");
        vhost = properties.getProperty("amqpVhost");
        queueName = properties.getProperty("queueName");
        exchange = properties.getProperty("exchange");
        routinKey = properties.getProperty("routinKey");

        dbHost = properties.getProperty("dbHost");
        dbPort = properties.getProperty("dbPort");
        dbUsername = properties.getProperty("dbUsername");
        dbPassword = properties.getProperty("dbPassword");
        dbName = properties.getProperty("dbName");

        StringScheme stringScheme = new StringScheme();
        TableLogScheme tableLogScheme = new TableLogScheme();

        SharedQueueWithBinding sharedQ = new SharedQueueWithBinding(queueName, exchange, routinKey);

        TopologyBuilder builder = new TopologyBuilder();

        AMQPSpout amqpSpout = new AMQPSpout(stringScheme, host, port, username,
                password, vhost, sharedQ, true, false);

        FilterLogBolt filterLogBolt = new FilterLogBolt(tableLogScheme);

        SaveLogBolt saveLogBolt = new SaveLogBolt(dbHost, dbPort, dbName, dbUsername, dbPassword, 2);

        builder.setSpout("amqp", amqpSpout, 10);

        builder.setBolt("filter", filterLogBolt, 10).shuffleGrouping("amqp");

        builder.setBolt("save", saveLogBolt, 10).shuffleGrouping("filter");

        Config config = new Config();

        if (args != null && args.length > 0) {
            config.setNumWorkers(20);
            config.setMaxSpoutPending(1000);
            config.setNumAckers(5);
            config.setMessageTimeoutSecs(30);//default
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            config.setDebug(true);
            config.setNumWorkers(10);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("XXOO", config, builder.createTopology());
            Utils.sleep(120000);//2 miniutes
            localCluster.killTopology("XXOO");
            localCluster.shutdown();
        }
    }
}
