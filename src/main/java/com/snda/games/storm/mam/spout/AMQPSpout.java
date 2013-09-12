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

public class AMQPSpout extends BaseRichSpout {

    private static final Logger _log = Logger.getLogger(AMQPSpout.class);

    private Scheme _scheme;
    private SpoutOutputCollector _collector;
    private static String ERROR_STREAM_NAME = "error-stream";

    public static final String CONFIG_PREFETCH_COUNT = "amqp.prefetch.count";
    private static final long DEFAULT_PREFETCH_COUNT = 500;
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
    private final boolean _autoAck;

    private transient Connection _amqpConnection;
    private transient Channel _amqpChannel;
    private transient QueueingConsumer _amqpConsumer;
    private transient String _amqpConsumerTag;

    public AMQPSpout(Scheme scheme, String host, int port,String username,
                     String password, String vhost, IQueueDeclaration queueDeclaration,
                     boolean requeueOnFail, boolean autoAck)
    {
        _scheme = scheme;

        _amqpHost = host;
        _amqpPort = port;
        _amqpUsername = username;
        _amqpPassword = password;
        _amqpVhost = vhost;

        _queueDeclaration = queueDeclaration;
        _requeueOnFail = requeueOnFail;
        _autoAck = autoAck;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
        declarer.declareStream(ERROR_STREAM_NAME, new Fields("deliveryTag", "bytes"));
    }

    /**
     * Called when task initialized in the worker to Provide environment.
     * Connect to the AMQP broker, declare queue and subscribes to messages.
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
            e.printStackTrace();
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

    @Override
    public void ack(Object msgId) {
        if (_autoAck == false && msgId instanceof Long) {
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

    @Override
    public void fail(Object msgId) {
        if (_autoAck == false && msgId instanceof Long) {
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

    @Override
    public void close() {
        try {
            if (_amqpChannel != null) {
                if (_amqpConsumerTag != null) {
                    _amqpChannel.basicCancel(_amqpConsumerTag);
                }
                _amqpChannel.close();
            }
        } catch (Exception e) {
            _log.warn("Error closing AMQP channel: " + e.getMessage());
        }
        try {
            if (_amqpConnection != null) {
                _amqpConnection.close();
            }
        } catch (Exception e) {
            _log.warn("Error closing AMQP connection: " + e.getMessage());
        }
    }

    private void connectAMQP() throws Exception {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        String connectUri = "amqp://" + _amqpUsername + ":" + _amqpPassword + "@"
                            + _amqpHost + ":" + _amqpPort + "/" + _amqpVhost;
        connectionFactory.setUri(connectUri);

        _amqpConnection = connectionFactory.newConnection();
        _amqpChannel = _amqpConnection.createChannel();

        _log.info("Setting basic.qos of channel prefetch-count to: " + _prefetchCount);
        _amqpChannel.basicQos(_prefetchCount);

        Queue.DeclareOk queue = _queueDeclaration.declare(_amqpChannel);
        String queueName = queue.getQueue();
        _log.info("Consuming queue " + queueName);

        _amqpConsumer = new QueueingConsumer(_amqpChannel);
        _amqpConsumerTag = _amqpChannel.basicConsume(queueName, _autoAck, _amqpConsumer);
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
