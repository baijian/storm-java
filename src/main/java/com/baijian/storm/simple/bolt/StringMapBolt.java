package com.baijian.storm.simple.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-16 4:00 PM
 * Desc: Split string to every single word to send as a new tuple.
 */
public class StringMapBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        for (String word : str.split(" ")) {
            _collector.emit(input, new Values(word));
        }
        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("string"));
    }
}
