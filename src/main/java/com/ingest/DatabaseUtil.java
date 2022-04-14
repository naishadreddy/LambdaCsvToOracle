package com.ingest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseUtil {

    public Connection getConnection(DatabaseCredentials dbCreds) {
        Connection connection = null;
        String url = "jdbc:oracle:thin:@" + dbCreds.getDbHost() + ":" + dbCreds.getDbPort() + "/" + "ORCL";
                //+ dbCreds.getDbName();
        try {
            connection = DriverManager.getConnection(url, dbCreds.getUserName(), dbCreds.getPassword());
            System.out.println("Created connection object successfully. Object: " + connection);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Could not get a connection to database.");
        }
        return connection;
    }
}