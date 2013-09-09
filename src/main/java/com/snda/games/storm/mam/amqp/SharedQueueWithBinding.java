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
        final Queue.DeclareOk queue = channel.queueDeclare(
            _queueName,
            false, /*non-durable*/
            false, /*non-exclusive*/
            false, /*non-auto-delete*/
            null
        );
        if (!_exchange.isEmpty()) {
            channel.exchangeDeclarePassive(_exchange);
            channel.queueBind(queue.getQueue(), _exchange, _routingKey);
        }
        return queue;
    }

    /*
    @Override
    public boolean isParallelConsumable() {
        return true;
    }
    */
}
