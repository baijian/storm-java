package com.snda.games.storm.mam.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import java.io.Serializable;

/**
 * Author: bj
 * Time: 2013-09-05 17:01
 * Desc:
 */
public interface IQueueDeclaration extends Serializable {

    AMQP.Queue.DeclareOk declare(Channel channel) throws Exception;

    boolean isParallelConsumable();
}
