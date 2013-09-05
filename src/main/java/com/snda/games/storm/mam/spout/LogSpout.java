package com.snda.games.storm.mam.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Author: bj
 * Time: 2013-09-04 18:01
 * Desc:
 */
public class LogSpout extends BaseRichSpout {

    private static final Logger _log = Logger.getLogger(LogSpout.class);

    private String _rmqHost;
    private int _rmqPort;

    private Scheme _scheme;

    private SpoutOutputCollector _collector;

    public LogSpout(Scheme scheme, String queueHost, int port) {
        _scheme = scheme;
        _rmqHost = queueHost;
        _rmqPort = port;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(_scheme.getOutputFields());
    }

    @Override
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
    }

    @Override
    public void nextTuple() {
    }

    public void ack() {

    }

    public void fail() {

    }
}
