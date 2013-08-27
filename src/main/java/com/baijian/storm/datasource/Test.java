package com.baijian.storm.datasource;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Author: bj
 * Time: 2013-08-27 10:28 AM
 * Desc:
 */
public class Test {

    public static void main(String[] args) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("config.properties"));

            System.out.println(prop.getProperty("username"));
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
