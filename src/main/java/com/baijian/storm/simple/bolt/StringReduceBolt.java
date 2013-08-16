package com.baijian.storm.simple.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-16 4:01 PM
 * Desc: calculate numbers of every word and write to mysql every miniute.
 */
public class StringReduceBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private Map<String, Integer> _counts;
    private long _t;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _counts = new HashMap<String, Integer>();
        _t = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = _counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        _counts.put(word, count);
        _collector.ack(input);
        long now = System.currentTimeMillis();
        if (now - _t > 60000) {
            writeToMysql(input);
            _t = now;
            _counts.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private void writeToMysql(Tuple tuple) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = null;
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            /*
            String baseSql = "insert into test (data) values ";
            String baseSql = "replace into test () values(),()";
            for (int i = 0; i < _counter; i++) {
                baseSql = baseSql + "(?),";
            }
            String sql = baseSql.substring(0, baseSql.length() - 1);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int j = 1; j <= _counter; j++) {
                preparedStatement.setString(j, data.get(j));
            }
            preparedStatement.execute();
            */
            connection.close();
        } catch (Exception e) {
            _collector.fail(tuple);
        }
    }
}
