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

<<<<<<< HEAD
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
=======
    public String downstreamMessage(String message) {
            if (message.equals("Start API")) {
                // run pipeline ingestion
                MSSQLReader mssqlReader = new MSSQLReader();
                JSONArray jsonObject = mssqlReader.fetchData();

                // run schemaDefinition
                //SchemaDefinitionFilter schemaDefinitionFilter = new SchemaDefinitionFilter(new DataTypeInfererUtil(), new S3DataWriter());
                //schemaDefinition.main(jsonObject.toJSONString());

                // upstream message
                return "Stop API";
            } else {
                return "Invalid message";
            }
        }
>>>>>>> 3813297a505e6a1262885d40143da5228e0c161e
}
