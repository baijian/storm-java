package com.baijian.storm.datasource;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class RabbitMQRecv {

    private final static String QUEUE_NAME = "recordlog";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /*
        If you have severial workers, RabbitMQ will delivery message
        in Round-robin model, if you do not set basicQos, it will
        delivery message one by one, maybe one worker get all the
        busy workers, then it will be not fair.
        So you shout set basicQos which means RabbitMQ will not delivery
        message to worker which have not ack the previous one.
        If all workers are busy, our queue will fill up, we should add
        workers or some other strategies. We should keep an eye on that.
         */
        int  prefetchCount = 500;
        channel.basicQos(prefetchCount);

        boolean durable = false;
        /*
        In Sender we have define the queue to be durable, so Receiver
        must also define as durable.
        So queue Declare code should be same with Sender.
         */
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        channel.exchangeDeclarePassive("recordlogs");
        channel.queueBind(QUEUE_NAME, "recordlogs","record");
        System.out.println("[*] Waiting for messages. To exit press CTRL+C");

        /*
        If severial consumers get message from the queue,
        the RabbitMQ will send message to worker in Round-robin dispatching.
         */
        QueueingConsumer consumer = new QueueingConsumer(channel);
        boolean autoAck = false;
        /*
        If autoAck set true,once RabbitMQ delivery a message to consumer,
        the message will be immediately removed from memory.
        If set true, RabbitMQ will redelivery the message only when worker
        connection die, and redelivery message to other workers,there are not
        any message timeouts concepts.
        So you want to delete one message from memory which is whole processed,
        your worker should an ack back to RabbitMQ.
         */
        channel.basicConsume(QUEUE_NAME, autoAck, consumer);

        while(true) {
            /*
            Consumer.nextDelivery() will block until another message has been delivered from the server.
             */
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            /*
            ack the message which have been received.
             */
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            System.out.println("[x] Received '" + message + "'");
        }
    }
}
