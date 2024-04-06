package org.healthetl.filters;

import java.sql.Statement;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class PostgresReader extends Filter {
    
    public void run() {
        // run fetch logic
        fetchData();
    }

    // JDBC URL, username, and password of PostgreSQL server
    private final String JDBC_URL = "jdbc:postgresql://localhost:5432/catcards";
    private final String JDBC_USER = "postgres";
    private final String JDBC_PASSWORD = "postgres1";

    public void fetchData() {

        // initialize json array
        JSONArray jsonArray = new JSONArray();

        // connect to DB url
        try (Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
        Statement statement = connection.createStatement();) {

            // select all records from the specified table
            String query = "SELECT * FROM " + "testimport";

            // execute the query as result set
            ResultSet resultSet = statement.executeQuery(query);

            // extract meta data of the results
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // iterate through columns and put each column into the JSON object
            while (resultSet.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    String columnValue = resultSet.getString(i);
                    jsonObject.put(columnName, columnValue);
                }
                // add the JSON object representing a row to the array
                jsonArray.add(jsonObject);
                output.write(jsonObject);
            }
            output.notifyThreads();
        } catch (SQLException e) {
            System.err.println("Failed to connect to the PostgreSQL server.");
            e.printStackTrace();
        }
    }
}
