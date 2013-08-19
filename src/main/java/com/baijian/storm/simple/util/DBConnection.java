package com.baijian.storm.simple.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.activation.DataSource;
import java.sql.Connection;
import java.util.ResourceBundle;

/**
 * Author: bj
 * Time: 2013-08-19 7:10 PM
 * Desc: Mysql connection pool using C3P0
 */
public class DBConnection {

    private static ComboPooledDataSource _dataSource;
    private static final String DRIVER_NAME;
    private static final String URL;
    private static final String USERNAME;
    private static final String PASSWORD;

    static {
        final ResourceBundle config = ResourceBundle.getBundle("databases.config");
        DRIVER_NAME = config.getString("driverName");
        URL = config.getString("url");
        USERNAME = config.getString("username");
        PASSWORD = config.getString("password");
        _dataSource = getDataSource();
    }

    public static Connection getConnection() throws Exception {
        return _dataSource.getConnection();
    }

    private static ComboPooledDataSource getDataSource() {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass(DRIVER_NAME);
        } catch (Exception e) {
        }
        cpds.setJdbcUrl(URL);
        cpds.setUser(USERNAME);
        cpds.setPassword(PASSWORD);
        cpds.setMinPoolSize(5);
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(20);
        return cpds;
    }
}
