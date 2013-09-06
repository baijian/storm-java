package com.snda.games.storm.mam.amqp;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;

public class SharedQueueWithBinding implements IQueueDeclaration {

    private final String _queueName;
    private final String _exchange;
    private final String _routingKey;

    public SharedQueueWithBinding(String queueName, String exchange,
                                  String routingKey) {
        _queueName = queueName;
        _exchange = exchange;
        _routingKey = routingKey;
    }

    @Override
    public Queue.DeclareOk declare(Channel channel) throws Exception {
        channel.exchangeDeclarePassive(_exchange);
        final Queue.DeclareOk queue = channel.queueDeclare(
            _queueName,
            true, /*durable*/
            false, /*non-exclusive*/
            false, /*non-auto-delete*/
            null
        );
        channel.queueBind(queue.getQueue(), _exchange, _routingKey);
        return queue;
    }

    @Override
    public boolean isParallelConsumable() {
        return true;
    }
}
