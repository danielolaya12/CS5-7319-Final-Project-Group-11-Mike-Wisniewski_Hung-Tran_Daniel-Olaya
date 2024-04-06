package org.healthetl;
import java.io.IOException;
import org.healthetl.connectors.C2;
import org.healthetl.filters.*;

public class Main {
    
    public static void main(String[] args) {
        String AWS_ACCESS_KEY = "AKIAV2RUR3AD4VVPWSZB";
        String AWS_SECRET_KEY = "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";
        String s3BucketName = "cs7319";

        setupReader(new CsvReader(), "Start CSV", "patients_c2");
        setupReader(new MSSQLReader(), "Start MSSQL", "medical_c2");
        setupReader(new PostgresReader(), "Start Postgres", "operations_c2");
        setupReader(new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, s3BucketName, "inbound/medications.csv"), "Start S3", "trials_c2");
        setupReader(new ApiReader(), "Start API", "regulatory_c2");
    }
    
    private static void setupReader(Filter reader, String messageString, String dataSource) {

        String loggerStringBegin = String.format("Start of %s", dataSource);
        MetaDataLogger.logMetaData(loggerStringBegin);

        final String s3BucketName = "cs7319";
        final String s3BasePath = String.format("/base/%s", dataSource);
        // final String s3ReadPath = String.format("base/%s/output.csv", dataSource);
        final String s3CuratedPath = String.format("/curated/%s", dataSource);
        final String s3SchemaPath = String.format("schema_log/%s/schema_definition.json", dataSource);
        // final S3SchemaWriter s3SchemaWriter = new S3SchemaWriter(s3BucketName, s3SchemaPath, AWS_ACCESS_KEY, AWS_SECRET_KEY);

        // set up C2 connector
        C2 C2Connector = new C2();
        try {
            String upstreamMessage = C2Connector.downstreamMessageBase(messageString, s3BucketName, s3SchemaPath, s3BasePath, dataSource);
            String baseLayer = C2Connector.upstreamMessageBase(upstreamMessage, dataSource);
            String curatedLayer = C2Connector.downstreamMessageCurated(baseLayer, s3BucketName, s3CuratedPath, dataSource);
            String endString = C2Connector.upstreamMessageCurated(curatedLayer, dataSource);
            System.out.println(endString);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
  

