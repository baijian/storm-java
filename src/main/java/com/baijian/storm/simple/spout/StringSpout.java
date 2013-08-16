package com.baijian.storm.simple.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Author: bj
 * Time: 2013-08-16 3:40 PM
 * Desc: Send strings every 100 milliseconds
 */
public class StringSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private Random _rand;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("strings"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String[] strings = new String[] {
            "baijian mail snda",
            "weibo snda gmail",
            "snda xx hello",
        };
        _collector.emit(new Values(strings[_rand.nextInt(strings.length)]));
    }

    public void ack() {
    }

    public void fail() {
    }
}
