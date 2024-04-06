package org.healthetl.connectors;

import org.healthetl.filters.*;
import org.json.simple.JSONArray;

public class C2 {

    public String upstreamMessage(String message) {
        if (message.equals("Stop API")) {
            // TODO: run parquetWriter
            

            // TODO: metaDataLogger
            

            System.out.println("Upstream Completed");
            // upstream message
            return "Stop API";
        } else {
            return "Invalid message";
        }
    }

    public String downstreamMessage(String message) {
            if (message.equals("Start API")) {
                // run pipeline ingestion
                MSSQLReader mssqlReader = new MSSQLReader();
                JSONArray jsonArray = mssqlReader.fetchData();

                // run schemaDefinition
                SchemaDefinition schemaDefinition = new SchemaDefinition();
                schemaDefinition.main(jsonArray.toJSONString());

                // output to base layer

                // upstream message
                return "Stop API";
            } else {
                return "Invalid message";
            }
        }
}
