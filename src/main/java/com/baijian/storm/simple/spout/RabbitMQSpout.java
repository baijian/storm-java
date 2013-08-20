package com.baijian.storm.simple.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Author: bj
 * Time: 2013-08-19 11:59 AM
 * Desc: Use RabbitMQ as storm spout.
 */
public class RabbitMQSpout extends BaseRichSpout {

    private static final Logger _log = Logger.getLogger(RabbitMQSpout.class);

    public static final String CONFIG_PREFETCH_COUNT = "amqp.prefetch.count";
    private static final long DEFAULT_PREFETCH_COUNT = 100;
    public static final long WAIT_FOR_NEXT_MESSAGE = 1L;
    public static final long WAIT_AFTER_SHUTDOWN_SIGNAL = 10000L;
    public static String ERROR_STREAM_NAME = "error-stream";
    private final String QUEUE_NAME = "rabbit";

    private int _prefetchCount;
    private final String _amqpHost;
    private final int _amqpPort;
    private final boolean _requeueOnFail;
    private final Scheme _scheme;

    private transient boolean _spoutActive = true;
    private transient Connection _amqpConnection;
    private transient Channel _amqpChannel;
    private transient QueueingConsumer _amqpConsumer;
    private transient String _amqpConsumerTag;

    private SpoutOutputCollector _collector;

    public RabbitMQSpout(String host, int port, boolean requeueOnFail, Scheme scheme) {
        _amqpHost = host;
        _amqpPort = port;
        _requeueOnFail = requeueOnFail;
        _scheme = scheme;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
        declarer.declareStream(ERROR_STREAM_NAME, new Fields("deliveryTag", "bytes"));
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        Long prefetchCount = (Long) map.get(CONFIG_PREFETCH_COUNT);
        if (prefetchCount == null) {
            prefetchCount = DEFAULT_PREFETCH_COUNT;
        } else if(prefetchCount < 1) {
            throw new IllegalArgumentException(CONFIG_PREFETCH_COUNT + " must be greater than 1");
        }
        _prefetchCount = prefetchCount.intValue();
        _collector = spoutOutputCollector;
        try {
            connectAMQP();
        } catch (Exception e) {
            _log.warn("Fail to connect to AMQP", e);
            Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);
            reconnect();
        }
    }

    @Override
    public void nextTuple() {
        if (_spoutActive && _amqpConsumer != null) {
            try {
                final QueueingConsumer.Delivery delivery = _amqpConsumer.nextDelivery(WAIT_FOR_NEXT_MESSAGE);
                if (delivery == null) {
                    return;
                }
                final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                final byte[] message = delivery.getBody();
                List<Object> msg = _scheme.deserialize(message);
                if (msg != null && msg.size() > 0) {
                    _collector.emit(msg, deliveryTag);
                } else {
                    handleBadMsg(deliveryTag, message);
                }
            } catch (Exception e) {
            }
        }
    }

    private void handleBadMsg(long deliveryTag, byte[] message) {
        _log.info("Bad message that is not schema " + deliveryTag);
        ack(deliveryTag);
        _collector.emit(ERROR_STREAM_NAME, new Values(deliveryTag, message));
    }

    public void ack(Object msgId) {
        final long deliveryTag = (Long) msgId;
        if (_amqpChannel != null) {
            try {
                _amqpChannel.basicAck(deliveryTag, false);
            } catch (Exception e) {
            }
        }
    }

    public void fail(Object msgId) {
        final long deliveryTag = (Long) msgId;
        if (_amqpChannel != null) {
            try {
                if (_amqpChannel.isOpen()) {
                    _amqpChannel.basicReject(deliveryTag, _requeueOnFail);
                } else {
                    reconnect();
                }
            }catch (Exception e) {
                _log.warn("Fail to reject delivery-tag " + deliveryTag, e);
            }
        }
    }

    public void close() {
        try {
            if (_amqpChannel != null) {
                if (_amqpConsumerTag != null) {
                    _amqpChannel.basicCancel(_amqpConsumerTag);
                }
                _amqpChannel.close();
            }
        } catch(Exception e) {
            _log.warn("Error closing AMQP channel", e);
        }
        try {
            if (_amqpConnection != null) {
                _amqpConnection.close();
            }
        }catch(Exception e){
            _log.warn("Error closing AMQP connection", e);
        }
    }

    public void activate() {
        _log.info("Unpausing spout");
        _spoutActive = true;
    }

    public void deactivate() {
        _log.info("Pausing spout");
        _spoutActive = false;
    }

    private void connectAMQP() throws Exception {
        final int prefetchCount = _prefetchCount;
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(_amqpHost);
        connectionFactory.setPort(_amqpPort);
        _amqpConnection = connectionFactory.newConnection();
        _amqpChannel = _amqpConnection.createChannel();
        _amqpChannel.basicQos(prefetchCount);
        _amqpChannel.queueDelete(QUEUE_NAME);
        _amqpConsumer = new QueueingConsumer(_amqpChannel);
        _amqpConsumerTag = _amqpChannel.basicConsume(QUEUE_NAME, false, _amqpConsumer);
    }

    private void reconnect() {
        _log.info("Reconnect to AMQP broker.");
        try {
            connectAMQP();
        } catch (Exception e) {
            _log.warn("Fail to reconnect to AMQP broker", e);
        }
    }
}
