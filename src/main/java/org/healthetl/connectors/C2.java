package org.healthetl.connectors;

import java.io.IOException;

import org.healthetl.filters.*;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;

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

    // public String downstreamMessage(String message) {
    //         if (message.equals("Start API")) {
    //             // run pipeline ingestion
    //             MSSQLPipeline mssqlPipeline = new MSSQLPipeline();
    //             JSONArray jsonObject = mssqlPipeline.fetchData();

    //             // run schemaDefinition
    //             SchemaDefinition schemaDefinition = new SchemaDefinition();
    //             schemaDefinition.main(jsonObject.toJSONString());

    //             // upstream message
    //             return "Stop API";
    //         } else {
    //             return "Invalid message";
    //         }
    //     }
}
