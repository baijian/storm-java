package com.snda.games.storm.mam.spout;


import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Author: bj
 * Time: 2013-09-04 23:49
 * Desc: Use RabbitMQ to be the spout of storm topology.
 */
public class AMQPSpout extends BaseRichSpout {

    private static final Logger _log = Logger.getLogger(AMQPSpout.class);

    private Scheme _scheme;
    private SpoutOutputCollector _collector;

    public static final String CONFIG_PREFETCH_COUNT = "amqp.prefetch.count";
    private static final long DEFAULT_PREFETCH_COUNT = 100;
    private int _prefetchCount;
    private final String _amqpHost;
    private final int _amqpPort;
    private final String _amqpUsername;
    private final String _amqpPassword;
    private final String _amqpVhost;

    private transient Connection _amqpConnection;
    private transient Channel _amqpChannel;
    private transient QueueingConsumer _amqpConsumer;
    private transient String _amqpConsumerTag;

    public AMQPSpout(Scheme scheme, String host, int port,
                     String username, String password, String vhost) {
        _scheme = scheme;
        _amqpHost = host;
        _amqpPort = port;
        _amqpUsername = username;
        _amqpPassword = password;
        _amqpVhost = vhost;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    /**
     * Called when task initialized in the worker to Provide environment.
     * Connect to the AMQP broker, declare queue and subscribes to messages.
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

        Long prefetchCount = (Long) conf.get(CONFIG_PREFETCH_COUNT);
        if (prefetchCount == null) {
            _log.info("Using default prefetch count");
            prefetchCount = DEFAULT_PREFETCH_COUNT;
        } else if(prefetchCount < 1) {
            prefetchCount = 1L;
        }
        _prefetchCount = prefetchCount.intValue();

        try {
            connectAMQP();
        } catch (Exception e) {
            _log.error("Connect to AMQP failed: " + e.getMessage());
        }
    }

    @Override
    public void nextTuple() {
    }

    public void ack() {

    }

    public void fail(Object msgId) {

    }

    private void connectAMQP() throws Exception {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(_amqpHost);
        connectionFactory.setPort(_amqpPort);
        connectionFactory.setUsername(_amqpUsername);
        connectionFactory.setPassword(_amqpPassword);
        connectionFactory.setVirtualHost(_amqpVhost);

        _amqpConnection = connectionFactory.newConnection();
        _amqpChannel = _amqpConnection.createChannel();
        _log.info("Setting basic.qos of channel prefetch-count to: " + _prefetchCount);
        _amqpChannel.basicQos(_prefetchCount);
    }
}
