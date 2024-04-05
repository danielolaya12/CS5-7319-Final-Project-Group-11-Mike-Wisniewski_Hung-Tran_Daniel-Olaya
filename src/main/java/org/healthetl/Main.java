package org.healthetl;

import org.healthetl.connectors.Pipe;
import org.healthetl.data.S3SchemaWriter;
import org.healthetl.filters.*;
import org.healthetl.utils.DataTypeInfererUtil;

public class Main {
    private static final String s3BucketName = "cs7319";
    private static final String s3SchemaPath = "schema_log/schema_definition.json";
    private static final DataTypeInfererUtil dataTypeInfererUtil = new DataTypeInfererUtil();
    private static final S3SchemaWriter s3SchemaWriter = new S3SchemaWriter(s3BucketName, s3SchemaPath);
    public static void main(String[] args) {
        setupReader(new CsvReader(), new Pipe(), "CSV");
        setupReader(new ApiReader(), new Pipe(), "API");
//        setupReader(new S3Reader("", "", "7319-software-architecture", "healthcare_dataset.csv"), new Pipe(), "S3");
//        setupReader(new MSSQLReader(), new Pipe(), "MSQL");
//        setupReader(new PostgresReader(), new Pipe(), "POSTGRES");


        /*
        //Meta Data Logger example
        MetaDataLogger.logMetaData("Start C2");

        // Setup MSSQL Pipe and Filter
        C2 mainC2 = new C2();

        // downstream messaging
        String downstreamMessage = mainC2.downstreamMessage("Start API");
        
        // upstream messaging
        String upstreamMessage = mainC2.upstreamMessage(downstreamMessage);

        System.out.println("All threads have completed");

        // S3 Writer
        String bucketName = "cs7319/curated/medical/data.csv";
        String accessKey = "";
        String secretKey = "";
        Regions region = Regions.US_EAST_1;
        S3Writer s3Writer = new S3Writer(bucketName, accessKey, secretKey, region);
        // Create a mock JSONObject
        JSONObject jsonData = new JSONObject();
        jsonData.put("name", "John");
        jsonData.put("age", 30);
        jsonData.put("city", "New York");
        // JSONObject jsonData =
        s3Writer.writeToS3(jsonData);
         */
    }
    private static void setupReader(Filter reader, Pipe pipe, String readerName) {
        SchemaDefinitionFilter schemaDefinitionFilter = new SchemaDefinitionFilter(dataTypeInfererUtil, s3SchemaWriter, readerName);
        //DataTypeInfererTest schemaDefinitionFilter = new DataTypeInfererTest(readerName);
        reader.setOut(pipe);
        schemaDefinitionFilter.setIn(pipe);
        Thread readerThread = new Thread(reader);
        Thread filterThread = new Thread(schemaDefinitionFilter);
        readerThread.start();
        filterThread.start();
    }
}


        // beginScheduler(while (time != 1PM)){
        //     {
            // Starts Pipelines
            //     Message msg_API = C2Connector_Downstream("Start API");
            //     Message msg_CSV = C2Connector_Downstream("Start CSV");
            //     Message msg_MSSQL = C2Connector_Downstream("Start MSSQL");

            // Stops Scheduler
            //     C2Connector_Upstream(msg_API);
            //     C2Connector_Upstream(msg_CSV);
            //     C2Connector_Upstream(msg_MSSQL);

        //         Filter[] filters = new Filter[] {new PostgresPipeline()};
        //         setOut(filters);
        //         startFilters(filters);
        //     }
            
        // }