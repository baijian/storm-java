package com.snda.games.storm.mam.spout;


import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.snda.games.storm.mam.amqp.IQueueDeclaration;
import org.apache.log4j.Logger;

import java.util.List;
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

    private static String ERROR_STREAM_NAME = "error-stream";

    public static final String CONFIG_PREFETCH_COUNT = "amqp.prefetch.count";
    private static final long DEFAULT_PREFETCH_COUNT = 100;
    private int _prefetchCount;

    private static final long WAIT_FOR_NEXT_MESSAGE = 1L;
    private static final long WAIT_AFTER_SHUTDOWN_SIGNAL = 10000L;

    private final String _amqpHost;
    private final int _amqpPort;
    private final String _amqpUsername;
    private final String _amqpPassword;
    private final String _amqpVhost;

    private final IQueueDeclaration _queueDeclaration;
    private final boolean _requeueOnFail;

    private transient Connection _amqpConnection;
    private transient Channel _amqpChannel;
    private transient QueueingConsumer _amqpConsumer;
    private transient String _amqpConsumerTag;

    public AMQPSpout(Scheme scheme, String host, int port,String username,
                     String password, String vhost, IQueueDeclaration queueDeclaration, boolean requeueOnFail) {
        _scheme = scheme;

        _queueDeclaration = queueDeclaration;
        _requeueOnFail = requeueOnFail;

        _amqpHost = host;
        _amqpPort = port;
        _amqpUsername = username;
        _amqpPassword = password;
        _amqpVhost = vhost;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
        declarer.declareStream(ERROR_STREAM_NAME, new Fields("deliveryTag", "bytes"));
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
        if (_amqpConsumer != null) {
            try {
                final QueueingConsumer.Delivery delivery = _amqpConsumer.nextDelivery(WAIT_FOR_NEXT_MESSAGE);
                if (delivery == null) {
                    return;
                }
                final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                final byte[] message = delivery.getBody();
                List<Object> deserializedMessage = _scheme.deserialize(message);
                if (deserializedMessage != null && deserializedMessage.size() > 0) {
                    _collector.emit(deserializedMessage, deliveryTag);
                } else {
                    handleMalformedDelivery(deliveryTag, message);
                }
            } catch (Exception e) {
                _log.warn("AMQP connection dropped, will attempt to reconnect...");
                Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);
                reconnect();
            }
        }
    }

    public void ack(Object msgId) {
        if (msgId instanceof Long) {
            final long deliveryTag = (Long) msgId;
            if (_amqpChannel != null) {
                try {
                    _amqpChannel.basicAck(deliveryTag, false);
                } catch (Exception e) {
                    _log.warn("Fail to ack delivery-tag " + deliveryTag + " : " + e.getMessage());
                }
            }
        } else {
            _log.warn(String.format("Do not know how to ack(%s: %s)", msgId.getClass().getName(), msgId));
        }
    }

    public void fail(Object msgId) {
        if (msgId instanceof Long) {
            final long deliveryTag = (Long) msgId;
            if (_amqpChannel != null) {
                try {
                    _amqpChannel.basicReject(deliveryTag, _requeueOnFail);
                } catch (Exception e) {
                    _log.warn("Fail to reject delivery-tag " + deliveryTag + " : " + e.getMessage());
                }
            }
        } else {
            _log.warn(String.format("Do not know how to reject(%s: %s)", msgId.getClass().getName(), msgId));
        }
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

        final Queue.DeclareOk queue = _queueDeclaration.declare(_amqpChannel);
        final String queueName = queue.getQueue();
        _log.info("Consuming queue " + queueName);

        _amqpConsumer = new QueueingConsumer(_amqpChannel);
        _amqpConsumerTag = _amqpChannel.basicConsume(queueName, false, _amqpConsumer);
    }

    private void reconnect() {
        _log.info("Reconnecting to AMQP broker...");
        try {
            connectAMQP();
        } catch (Exception e) {
            _log.warn("Fail to reconnect to AMQP brker : " + e.getMessage());
        }
    }

    private void handleMalformedDelivery(long deliveryTag, byte[] message) {
        _log.info("Malformed deserialized message: " + deliveryTag);
        ack(deliveryTag);
        _collector.emit(ERROR_STREAM_NAME, new Values(deliveryTag, message));
    }
}
