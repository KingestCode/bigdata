package com.rox.spark.java.sql_hive_spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ThriftServerClientJava_thriftServer访问hive {
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection conn = DriverManager.getConnection("jdbc:hive2://cs1:10000","ap","123");

        Statement st = conn.createStatement();

        ResultSet rs = st.executeQuery("select * from tt where age > 12 ORDER BY age DESC ");

        while (rs.next()) {
            int id = rs.getInt(1);
            String name = rs.getString(2);
            int age = rs.getInt(3);
            System.out.println(id + "," + name + "," + age);
        }
        rs.close();
    }
}
