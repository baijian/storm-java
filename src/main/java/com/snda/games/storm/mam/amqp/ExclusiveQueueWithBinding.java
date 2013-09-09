package com.snda.games.storm.mam.amqp;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;

public class ExclusiveQueueWithBinding implements IQueueDeclaration {

    private final String _exchange;
    private final String _routingKey;

    public ExclusiveQueueWithBinding(String exchange, String routingKey) {
        _exchange = exchange;
        _routingKey = routingKey;
    }

    @Override
    public Queue.DeclareOk declare(Channel channel) throws Exception {
        channel.exchangeDeclarePassive(_exchange);
        final Queue.DeclareOk queue = channel.queueDeclare();
        channel.queueBind(queue.getQueue(), _exchange, _routingKey);
        return queue;
    }

    /*
    @Override
    public boolean isParallelConsumable() {
        return false;
    }
    */
}
