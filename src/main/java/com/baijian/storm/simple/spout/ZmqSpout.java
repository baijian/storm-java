package com.baijian.storm.simple.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.zeromq.ZMQ;

import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-15 5:59 PM
 * Desc:
 */
public class ZmqSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket sub = context.socket(ZMQ.SUB);
        sub.setIdentity("hello".getBytes());
        sub.connect("tcp://localhost:5678");
        sub.subscribe("".getBytes());
        while(true) {
            String msg = new String(sub.recv(0));
            if (msg.length() > 0) {
                _collector.emit(new Values(msg));
            }
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("XXOO"));
    }
}
