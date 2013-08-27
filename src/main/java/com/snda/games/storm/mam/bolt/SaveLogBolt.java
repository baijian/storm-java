package com.snda.games.storm.mam.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-27 3:52 PM
 * Desc: Save some log to mysql.
 */
public class SaveLogBolt extends BaseRichBolt {

    private OutputCollector _collector;

    private String _host;
    private String _port;
    private String _db;
    private String _username;
    private String _password;

    public SaveLogBolt(String host, String port, String db,
                       String username, String password) {
        _host = host;
        _port = port;
        _db = db;
        _username = username;
        _password = password;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String log = input.getString(0);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private void writeLog() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = null;
            String connect_str = "jdbc:mysql://" + _host + ":" +  _port + "/" + _db;
            connection = DriverManager.getConnection(connect_str, _username, _password);
            String sql = "insert into rlog(name, val, value) values(?,?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, "");
            preparedStatement.setString(2, "");
            preparedStatement.setString(3, "");
            preparedStatement.execute();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
        }
    }
}
