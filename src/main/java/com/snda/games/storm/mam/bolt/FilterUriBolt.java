package com.snda.games.storm.mam.bolt;

import backtype.storm.spout.Scheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Author: bj
 * Time: 2013-08-21 3:02 PM
 * Desc: Filter url which are registered in mysql and synchronous urls every 3 miniute.
 */
public class FilterUriBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private long _t;
    private Map<Integer, String> _uris;
    private Scheme _scheme;

    private String _host;
    private String _port;
    private String _db;
    private String _username;
    private String _password;

    public FilterUriBolt(Scheme scheme, String host, String port, String db,
                         String username, String password) {
        _uris = new HashMap<Integer, String>();
        _t = 0;
        _scheme = scheme;

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
        long now = System.currentTimeMillis();
        if (_t == 0 || (now - _t) > 180000) {
            getUri();
            _t = now;
        }

        String log = input.getString(0);
        String[] logs = log.split(" ");

        String time_local = logs[3].substring(1, logs[3].length());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
        Date dt = null;
        try {
            dt = simpleDateFormat.parse(time_local);
        } catch (ParseException e) {
        }
        SimpleDateFormat transFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String t_str = transFormat.format(dt);

        String uri = logs[6];

        if (!_uris.isEmpty()) {
            Iterator it = _uris.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<Integer, String> u = (Map.Entry)it.next();
                if (Pattern.matches(u.getValue(), uri)) {
                    _collector.emit(new Values(u.getKey(), t_str));
                    break;
                }
            }
        }

        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    private void getUri() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = null;
            String connect_str = "jdbc:mysql://" + _host + ":" + _port + "/" + _db;
            connection = DriverManager.getConnection(connect_str, _username, _password);
            String sql = "select id,uri from url";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet rs = preparedStatement.executeQuery();
            if (!rs.wasNull()) {
                _uris.clear();
                while(rs.next()) {
                    _uris.put(rs.getInt("id"), rs.getString("uri"));
                }
            }
            rs.close();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
        }
    }
}
