package com.baijian.storm.datasource;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


public class RabbitMQSend {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        String message = "Hello World!";

        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println("[X] Send '" + message + "'");

        channel.close();
        connection.close();
    }

}
