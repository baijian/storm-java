package com.baijian.storm.datasource;

import com.rabbitmq.client.*;


/**
 * Author: bj
 * Time: 2013-08-19 11:20 AM
 * Desc:
 */
public class RabbitMQConfirm {

    static int msgCount = 10000;
    final static String QUEUE_NAME = "confirm";
    static ConnectionFactory connectionFactory;

    public static void main(String[] args) throws Exception {
        if(args.length > 0) {
            msgCount = Integer.parseInt(args[0]);
        }
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        (new Thread(new Consumer())).start();
        (new Thread(new Publisher())).start();
    }

    static class Publisher implements Runnable {
        public void run() {
            try {
                long startTime = System.currentTimeMillis();
                Connection connection = connectionFactory.newConnection();
                Channel ch = connection.createChannel();
                ch.queueDeclare(QUEUE_NAME, true, false, false, null);
                ch.confirmSelect();
                for (long i = 0; i < msgCount; i++) {
                    ch.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, "test".getBytes());
                }
                ch.waitForConfirmsOrDie();
                ch.queueDelete(QUEUE_NAME);
                ch.close();
                connection.close();
                long endTime = System.currentTimeMillis();
                System.out.printf("Test took %.3fs\n", (float)(endTime - startTime)/1000);
            } catch(Throwable e) {
                System.out.println(e.getMessage());
            }
        }
    }

    static class Consumer implements Runnable {
        public void run() {
            try {
                Connection connection = connectionFactory.newConnection();
                Channel ch = connection.createChannel();
                ch.queueDeclare(QUEUE_NAME, true, false, false, null);
                QueueingConsumer consumer = new QueueingConsumer(ch);
                ch.basicConsume(QUEUE_NAME, true, consumer);
                for (int i = 0; i < msgCount; i++) {
                    System.out.println(new String(consumer.nextDelivery().getBody()));
                }
                ch.close();
                connection.close();
            } catch (Throwable e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
