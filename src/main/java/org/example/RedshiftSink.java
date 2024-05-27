package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class RedshiftSink extends RichSinkFunction<String> {
    private transient Connection connection;
    private transient PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Set up the connection to Amazon Redshift
        connection = DriverManager.getConnection(
                "jdbc:redshift://your-redshift-cluster:5439/yourdb", "user", "password");
        String sql = "INSERT INTO your_table (id, name, value) VALUES (?, ?, ?)";
        statement = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        // Parse the CSV string
        String[] fields = value.split(",");
        int id = Integer.parseInt(fields[0]);
        String name = fields[1];
        String newValue = fields[2];

        // Set the values and execute the insert
        statement.setInt(1, id);
        statement.setString(2, name);
        statement.setString(3, newValue);
        statement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}