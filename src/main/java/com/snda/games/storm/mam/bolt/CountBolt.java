package com.snda.games.storm.mam.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-21 3:03 PM
 * Desc: Save count of each registered url to mysql every miniute.
 */
public class CountBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private Map<Integer, String> _timer;
    private Map<Integer, Integer> _counter;

    public CountBolt() {
        _counter = new HashMap<Integer, Integer>();
        _timer = new HashMap<Integer, String>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int url_id = input.getInteger(0);
        String time_str = input.getString(1);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dt = null;
        try {
            dt = simpleDateFormat.parse(time_str);
        } catch (Exception e) {
        }

        SimpleDateFormat miniuteFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String miniute_begin = miniuteFormat.format(dt);
        String miniute_begin_time = miniute_begin + ":00";

        if (!_timer.containsKey(url_id)) {
            _timer.put(url_id, miniute_begin_time);
        }
        if (!_counter.containsKey(url_id)) {
            _counter.put(url_id, 0);
        }

        Date dt_miniute_begin = null;
        try {
            dt_miniute_begin = miniuteFormat.parse(_timer.get(url_id));
        } catch (Exception e) {
        }

        if (dt.getTime() - dt_miniute_begin.getTime() > 60000) {
            recordCount(url_id, _timer.get(url_id), _counter.get(url_id));
            _timer.remove(url_id);
            _timer.put(url_id, miniute_begin_time);
            _counter.remove(url_id);
            _counter.put(url_id, 0);
        }

        if (_counter.containsKey(url_id)) {
            int count = _counter.get(url_id);
            count++;
            _counter.remove(url_id);
            _counter.put(url_id, count);
        }

        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private void recordCount(int url_id, String time, int count) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = null;
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            String sql = "insert into alog(url_id, time, count) values(?,?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setInt(1, url_id);
            preparedStatement.setString(2, time);
            preparedStatement.setInt(3, count);
            preparedStatement.execute();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
        }
    }
}
