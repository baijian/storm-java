package com.snda.games.storm.mam.bolt.logrecord;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SaveLogBolt extends BaseRichBolt {

    private OutputCollector _collector;

    private String _host;
    private String _port;
    private String _db;
    private String _username;
    private String _password;
    private Integer _count;

    private long _t;
    private Map<String, String> _tables;
    private Map<String, Integer> _counter;
    private Map<String, List<String>> _values;

    public SaveLogBolt(String host, String port, String db,
                        String username, String password, Integer count) {
        _host = host;
        _port = port;
        _db = db;
        _username = username;
        _password = password;
        _count = count;

        _t = 0;
        _tables = new HashMap<String, String>();
        _counter = new HashMap<String, Integer>();
        _values = new HashMap<String, List<String>>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long now = System.currentTimeMillis();
        if (_t == 0 || (now - _t) > 180000) {
            syncTableInfo();
            _t = now;
        }
        String tableName = input.getString(0);
        String logs = input.getString(1);
        if (_tables.containsKey(tableName)) {
            if (_counter.containsKey(tableName)) {
                Integer num = _counter.get(tableName);
                num++;
                _counter.remove(tableName);
                _counter.put(tableName, num);
                updateLogList(tableName, logs);
            } else {
                _counter.put(tableName, 1);
                updateLogList(tableName, logs);
            }
            if (_counter.get(tableName) >= _count) {
                writeLog(tableName);
                _counter.remove(tableName);
                _counter.put(tableName, 0);
                _values.remove(tableName);
            }
        }
        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private void updateLogList(String tableName, String logs) {
        List<String> logList;
        if (_values.containsKey(tableName)) {
            logList = _values.get(tableName);
            logList.add(logs);
            _values.remove(tableName);
            _values.put(tableName, logList);
        } else {
            logList = new ArrayList<String>();
            logList.add(logs);
            _values.put(tableName, logList);
        }
    }

    private void syncTableInfo() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = null;
            String connect_str = "jdbc:mysql://" + _host + ":" + _port + "/" + _db;
            connection = DriverManager.getConnection(connect_str, _username, _password);
            String sql = "select tablename,fieldsname from registerlog";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet rs = preparedStatement.executeQuery();
            if (!rs.wasNull()) {
                _tables.clear();
                while(rs.next()) {
                    _tables.put(rs.getString("tablename"), rs.getString("fieldsname"));
                }
            }
            rs.close();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeLog(String tableName) {
        String fieldsStr = _tables.get(tableName);
        String[] fields = fieldsStr.split(",");
        String field = "";
        for(int i = 0; i < fields.length; i++) {
            field += fields[i] + ",";
        }
        field = field.substring(0, field.length() - 1);
        String sql = "insert into " + tableName + "(" + field + ") values";
        List<String> listJson = _values.get(tableName);
        JSONParser jsonParser = new JSONParser();
        for (String jsonStr : listJson) {
            String fieldsVal = "";
            JSONObject jo = new JSONObject();
            try {
                jo = (JSONObject) jsonParser.parse(jsonStr);
                for (int j = 0; j < fields.length; j++) {
                    fieldsVal += "'" + (String)jo.get(fields[j]) + "',";
                }
            } catch (Exception e) {
            }
            fieldsVal = fieldsVal.substring(0, fieldsVal.length() - 1);
            fieldsVal = "(" + fieldsVal + "),";
            sql += fieldsVal;
        }
        sql = sql.substring(0, sql.length() - 1);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = null;
            String connect_str = "jdbc:mysql://" + _host + ":" +  _port + "/" + _db;
            connection = DriverManager.getConnection(connect_str, _username, _password);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
        }
    }
}
