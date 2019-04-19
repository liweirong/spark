package com.iris.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * @author iris
 * @date 2019/4/19
 */
public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;
    private static Integer poolSize = 5;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获得连接
     *
     * @return Connection
     */
    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<>();
                for (int i = 0; i < poolSize; i++) {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://localhost:3306/bigdata?characterEncoding=utf8&useSSL=true",
                            "root",
                            "13755886712Lwrong"
                    );
                    connectionQueue.push(conn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}
