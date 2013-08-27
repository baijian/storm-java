package com.baijian.storm.datasource;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.io.FileInputStream;
import java.util.Properties;


/**
 * Author: bj
 * Time: 2013-08-27 10:28 AM
 * Desc:
 */
public class Test {


    public static void main(String[] args) throws ParseException {
        Map<Integer, String> _uris = new HashMap<Integer, String>();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = null;
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            String sql = "select id,uri from url";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet rs = preparedStatement.executeQuery();
            if (!rs.wasNull()) {
                _uris.clear();
                while(rs.next()) {
                    _uris.put(rs.getInt("id"), rs.getString("uri"));
                }
            }
            rs.close();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
        }

        String log = "10.31.22.67 - - [22/Aug/2013:14:58:20 +0800] \"GET /index.php?g=Api&m=Malogin&phoneid=18677054841 HTTP/1.1\" 200 45 \"-\" \"-\"";
        System.out.println(log);
        String[] logs = log.split(" ");
        /*
        for (String s : logs) {
            System.out.println(s);
        }
        String s = logs[3].substring(1, logs[3].length());
        System.out.println(s);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
        Date dt = simpleDateFormat.parse(s);

        long tt = dt.getTime();
        System.out.println(tt);

        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String t_str = simpleDateFormat1.format(dt);
        System.out.println(t_str);
        */
        System.out.println(logs[6]);
        System.out.println(_uris.get(3));
//        if (Pattern.matches("^/index\\.php\\?g=Api&m=Malogin&phoneid=\\d+$", logs[6])) {
        if (Pattern.matches(_uris.get(3), logs[6])) {
            System.out.println("suc");
        } else {
            System.out.println("fail");
        }
    }

    public static void mainbak(String[] args) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("config.properties"));

            System.out.println(prop.getProperty("username"));
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
