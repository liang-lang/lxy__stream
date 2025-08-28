package com.v3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class aaa {
    private static Connection dbConn = null;
    public static void main(String[] args) {
        String dbURL = "jdbc:sqlserver://cdh02:1433;database=realtime;trustServerCertificate=true;";//这里输入自己的数据库名称（即将Stu改为你自己的数据库名称）其余都可以不做修改
        try {
            //1.加载驱动
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            System.out.println("加载驱动成功！");
            //2.连接
            dbConn = DriverManager.getConnection(dbURL, "sa", "Xy0511./");//这里的密码改为第一步你所修改的密码，用户名一般就为"sa"
            System.out.println("连接数据库成功！");
            String sql = "select * from realtime.dbo.cdc_test";//这个语句就是表的查询语句，按照你所建的表修改名称
            PreparedStatement statement = null;
            statement = dbConn.prepareStatement(sql);
            ResultSet res = null;
            res = statement.executeQuery();
            while (res.next()) {
                String title = res.getString("name");
                System.out.println(title);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("连接数据库失败！");
        }
    }
}
