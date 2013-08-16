package com.baijian.storm.simple.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-15 2:23 PM
 * Desc: Print tuple info after a period of time
 */
public class PersistentDataBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private int _counter;
    private Map<Integer, String> data = new HashMap<Integer, String>();

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;
        _counter = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        _counter++;
        data.put(_counter, tuple.getString(0));
        if (_counter > 9) {
            writeToMysql(tuple);
        }
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private void writeToMysql(Tuple tuple) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = null;
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            String baseSql = "insert into test (data) values ";
            for (int i = 0; i < _counter; i++) {
                baseSql = baseSql + "(?),";
            }
            String sql = baseSql.substring(0, baseSql.length() - 1);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int j = 1; j <= _counter; j++) {
                preparedStatement.setString(j, data.get(j));
            }
            preparedStatement.execute();
            connection.close();
        } catch (Exception e) {
            _collector.fail(tuple);
        }
        _counter = 0;
        data.clear();
    }
}
