package com.baijian.storm.simple.bolt;

import backtype.storm.spout.Scheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.baijian.storm.simple.util.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-19 5:18 PM
 * Desc: Persist data to mysql database.
 */
public class MysqlPersistenceBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private final Scheme _scheme;

    public MysqlPersistenceBolt(Scheme scheme) {
        _scheme = scheme;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String log = tuple.getString(0);
        //TO DO mysql事务提交
        try {
            persistence(log);
        } catch (Exception e) {
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    private void persistence(String log) throws Exception {
        Connection connection = DBConnection.getConnection();
        String sql = "insert into";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, "");
        preparedStatement.execute();
        preparedStatement.close();
        connection.close();
    }
}
