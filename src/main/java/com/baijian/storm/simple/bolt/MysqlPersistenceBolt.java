package com.baijian.storm.simple.bolt;

import backtype.storm.spout.Scheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.baijian.storm.simple.util.DBConnection;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-19 5:18 PM
 * Desc: Persist data to mysql database.
 */
public class MysqlPersistenceBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private final Scheme _scheme;
    private int _counter;
    private Map<Integer, Tuple> _data = new HashMap<Integer, Tuple>();

    public MysqlPersistenceBolt(Scheme scheme) {
        _scheme = scheme;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        _counter++;
        _data.put(_counter, tuple);
        if(_counter > 9) {
            try {
                persistence();
            } catch (Exception e) {
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    private void persistence() throws Exception {
        Connection connection = DBConnection.getConnection();
        Statement stmt = connection.createStatement();
        for(int j = 0; j < _counter; j++) {
            Tuple tuple = _data.get(j);
            String sql = "insert into data() values()";
            stmt.addBatch(sql);
        }
        stmt.executeBatch();
        connection.commit();
        stmt.close();
        connection.close();
        for(int i = 0; i < _counter; i++) {
            _collector.ack(_data.get(i));
        }
        _counter = 0;
        _data.clear();
    }
}
