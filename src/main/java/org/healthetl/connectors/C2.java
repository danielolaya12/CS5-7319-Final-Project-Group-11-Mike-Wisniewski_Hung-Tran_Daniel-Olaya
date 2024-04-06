package org.healthetl.connectors;

import java.io.IOException;

import org.healthetl.data.S3SchemaWriter;
import org.healthetl.filters.*;
import org.healthetl.utils.DataTypeInfererUtil;
import org.json.simple.JSONArray;

import com.amazonaws.regions.Regions;

public class C2 {

    private static final Regions AWS_REGION = Regions.US_EAST_1;
    private static final String AWS_ACCESS_KEY = "AKIAV2RUR3AD4VVPWSZB";
    private static final String AWS_SECRET_KEY = "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";

    public String upstreamMessageBase(String message, String dataSource) {

        String inputString =  String.format("Stop %s", dataSource);
        String returnMessage = String.format("End of Base Layer %s", dataSource);

        if (message.equals(inputString)) {

            // metaDataLogger
            String loggerStringEnd = String.format("End of %s", dataSource);
            MetaDataLogger.logMetaData(loggerStringEnd);

            // upstream message
            return returnMessage;
        } else {
            return "Invalid Message";
        }
    }

    public String downstreamMessageBase(String message, String s3BucketName, String s3SchemaPath, String s3BasePath, String dataSource) throws IOException, InterruptedException {
        final DataTypeInfererUtil dataTypeInfererUtil = new DataTypeInfererUtil();
        final S3SchemaWriter s3SchemaWriter = new S3SchemaWriter(s3BucketName, s3SchemaPath, AWS_ACCESS_KEY, AWS_SECRET_KEY);
        final S3Writer s3WriterBase = new S3Writer(s3BucketName, s3BasePath, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
        final SchemaDefinitionFilter schemaDefinitionFilter = new SchemaDefinitionFilter(dataTypeInfererUtil, s3SchemaWriter);
        final String stopMessage = String.format("Stop %s", dataSource);

        if (message.equals("Start MSSQL")) {
            // run pipeline ingestion
            MSSQLReader mssqlReader = new MSSQLReader();
            JSONArray jsonArray = mssqlReader.fetchData();

            // run schemaDefinition
            schemaDefinitionFilter.schemaLog(jsonArray);

            // write to base
            s3WriterBase.writeToS3(jsonArray);

            // upstream message
            return stopMessage;
        }
        if (message.equals("Start API")) {
            // run pipeline ingestion
            ApiReader apiReader = new ApiReader();
            JSONArray jsonArray = apiReader.callPatientsApi();

            // run schemaDefinition
            schemaDefinitionFilter.schemaLog(jsonArray);

            // write to base
            s3WriterBase.writeToS3(jsonArray);

            // upstream message
            return stopMessage;
        } 
        if (message.equals("Start CSV")) {
            // run pipeline ingestion
            CsvReader csvReader = new CsvReader();
            JSONArray jsonArray = csvReader.readCsv();

            // run schemaDefinition
            schemaDefinitionFilter.schemaLog(jsonArray);

            // write to base
            s3WriterBase.writeToS3(jsonArray);

            // upstream message
            return stopMessage;
        } 
        if (message.equals("Start Postgres")) {
            // run pipeline ingestion
            PostgresReader postgresReader = new PostgresReader();
            JSONArray jsonArray = postgresReader.fetchData();

            // run schemaDefinition
            schemaDefinitionFilter.schemaLog(jsonArray);

            // write to base
            s3WriterBase.writeToS3(jsonArray);

            // upstream message
            return stopMessage;
        }
        if (message.equals("Start S3")) {
            // run pipeline ingestion
            S3Reader s3Reader = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, "cs7319", "inbound/medications.csv");
            JSONArray jsonArray = s3Reader.fetchData();

            // run schemaDefinition
            schemaDefinitionFilter.schemaLog(jsonArray);

            // write to base
            s3WriterBase.writeToS3(jsonArray);

            // upstream message
            return stopMessage;
        } 
        else {
            return "Invalid message";
        }
    }

    public String downstreamMessageCurated(String message, String s3BucketName, String s3CuratedPath, String dataSource){
        String inputString =  String.format("End of Base Layer %s", dataSource);
        final String stopMessage = String.format("Stop %s", dataSource);
        final String keyPath = String.format("base/%s/output.csv", dataSource);
        final S3Writer s3WriterCurated = new S3Writer(s3BucketName, s3CuratedPath, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

        if (message.equals(inputString)){
            // read in from base layer
            S3Reader s3Reader = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, "cs7319", keyPath);
            JSONArray jsonArray = s3Reader.fetchData();

            // output to curated layer
            s3WriterCurated.writeToS3(jsonArray);

            return stopMessage;
        } else {
            return "Invalid Message in Curated Read";
        }
    }

    public String upstreamMessageCurated(String message, String dataSource) {

        String inputString =  String.format("Stop %s", dataSource);
        String returnMessage = String.format("End of Curated Layer %s", dataSource);

        if (message.equals(inputString)) {

            // metaDataLogger
            String loggerStringEnd = String.format("End of %s", dataSource);
            MetaDataLogger.logMetaData(loggerStringEnd);

            System.out.println("Upstream Curated Layer Completed");
            
            // upstream message
            return returnMessage;
        } else {
            return "Invalid Message";
        }
    }
}
