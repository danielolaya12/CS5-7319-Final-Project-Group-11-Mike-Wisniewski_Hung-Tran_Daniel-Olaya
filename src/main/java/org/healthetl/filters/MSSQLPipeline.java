package org.healthetl.filters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import netscape.javascript.JSObject;

public class MSSQLPipeline extends Filter {

    // main run
    public void run() {
        // run fetch logic
        fetchData();
    }

    // jdbc url for connecting to SQL Server
    private final String DB_URL = "jdbc:sqlserver://DESKTOP-BBB6R7K;databaseName=medical;integratedSecurity=true;trustServerCertificate=true";

    public void fetchData() {

        // initialize json array
        JSONArray jsonArray = new JSONArray();

        try (// connect to DB url
            Connection conn = DriverManager.getConnection(DB_URL);
            Statement statement = conn.createStatement();) {

                // select all records from the specified table
                String query = "SELECT * FROM " + "all_prevalences";

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
                    output.notifyThreads();
                }
 
                System.out.println(jsonArray);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }