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
import java.util.List;
import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-21 10:43 AM
 * Desc: Count pv and 90% speed of every miniute.
 */
public class PvSpeedBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private final Scheme _scheme;
    private long _t;
    private int _pv;
    private List<Integer> _request_time;

    public PvSpeedBolt(Scheme scheme) {
        _scheme = scheme;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _t = System.currentTimeMillis();
        _pv = 0;
    }

    @Override
    public void execute(Tuple input) {
        /**
         * 2013-08-21 21:00:00
         */
        _pv++;
        _collector.ack(input);
        long now = System.currentTimeMillis();
        if (now - _t > 60000) {
            try {
                record();
            } catch (Exception e) {
            }
            _t = now;
            _pv = 0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    private void record() throws Exception {
        Connection connection = DBConnection.getConnection();
        String sql = "insert into ";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.execute();
        connection.commit();
        stmt.close();
        connection.close();
    }
}
