package com.baijian.storm.datasource;

import backtype.storm.spout.KestrelThriftClient;
import org.apache.thrift7.TException;

import java.io.IOException;
import java.util.Random;

/**
 * Author: bj
 * Time: 2013-08-15 5:08 PM
 * Desc:
 */
public class AddDataToKestrel {

    public static void main(String[] args) throws TException, IOException {
        KestrelThriftClient client = new KestrelThriftClient("localhost", 7000);
        queueSentenceItems(client, "test");
    }

    private static void queueSentenceItems(KestrelThriftClient client, String queueName)
            throws IOException, TException {
        String[] words = new String[] {
                "hello world",
                "I love the world",
                "You are good",
        };
        Random _rand = new Random();
        for (int i = 0; i <= 10; i++) {
            String word  = words[_rand.nextInt(words.length)];
            String val = i + "-" + word;
            client.put(queueName, val, 200);
        }
    }
}
