package net.sw.search.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBHepler {
    public static final String url="jdbc:mysql://localhost:3306/postition?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai";
    public static final String userName="root";
    public static final String userPassword="admin";
    public static final String className="com.mysql.cj.jdbc.Driver";
    public static Connection connection=null;


    public static Connection getConn(){
        try {
           Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection= DriverManager.getConnection(url,userName,userPassword);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }



}
