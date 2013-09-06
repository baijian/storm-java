package com.snda.games.storm.mam.bolt.logrecord;

import backtype.storm.spout.Scheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.Map;

/**
 * Author: bj
 * Time: 9/4/13 12:08 PM
 * Desc: Get table name in the json and emit the
 *      tableName and log json string.
 */
public class FilterLogBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private Scheme _scheme;

    public FilterLogBolt(Scheme scheme) {
        _scheme = scheme;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String logs = tuple.getString(0);
        JSONParser jsonParser = new JSONParser();
        String tableName = "";
        String logsStr = "";
        try {
            Object logsObj = jsonParser.parse(logs);
            JSONObject logsJbj = (JSONObject) logsObj;
            tableName = (String)logsJbj.get("tableName");
            logsJbj.remove("tableName");
            logsStr = logsJbj.toJSONString();
        } catch (Exception e) {
        }
        if (tableName.length() > 0 && logsStr.length() > 0) {
            _collector.emit(new Values(tableName, logsStr));
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        _scheme.getOutputFields();
    }
}
