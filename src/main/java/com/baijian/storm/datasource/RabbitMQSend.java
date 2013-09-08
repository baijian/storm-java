package com.baijian.storm.datasource;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;


public class RabbitMQSend {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        boolean durable = true;
        /*
        If durable set true, RabbitMQ will never lose the queue when
        RabbitMQ is quits or crash even your server is restarted.
         */
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

        String message = "Hello World!";

        /*
        As queue declare to be durable,so we need to mark the message
        as persistent by setting PERSISTENT_TEXT_PLAIN.
         */
        channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes());
        System.out.println("[X] Send '" + message + "'");

        channel.close();
        connection.close();
    }

}
