package org.healthetl;

import com.amazonaws.regions.Regions;

import org.healthetl.connectors.C2;
import org.healthetl.connectors.Pipe;
import org.healthetl.data.S3SchemaWriter;
import org.healthetl.filters.*;
import org.healthetl.utils.DataTypeInfererUtil;

public class Main {
    
    private static final DataTypeInfererUtil dataTypeInfererUtil = new DataTypeInfererUtil();
    
    public static void main(String[] args) {
        String AWS_ACCESS_KEY = "";
        String AWS_SECRET_KEY = "";
        String s3BucketName = "cs7319";


        C2 MSSQLC2 = new C2();
        String upstreamMessage = MSSQLC2.downstreamMessage("Start API");
        System.out.println(upstreamMessage);
        // setupReader(new CsvReader(), new Pipe(), "patients_pf");
        // setupReader(new MSSQLReader(), new Pipe(), "medical_pf");
        // setupReader(new PostgresReader(), new Pipe(), "operations_pf");
        // setupReader(new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, s3BucketName, "inbound/medications.csv"), new Pipe(), "trials_pf");
        // setupReader(new ApiReader(), new Pipe(), "regulatory_pf");

    }
    
    private static void setupReader(Filter reader, Pipe pipe, String dataSource) {

        String loggerStringBegin = String.format("Start of %s", dataSource);
        MetaDataLogger.logMetaData(loggerStringBegin);

        // static "global" variables
        final String BUCKETNAME_MSSQL_BASE = "medical_pf";
        final String BUCKETNAME_POSTGRES_CURATED = "operations_pf";
        final String CSV_NAME = "patient_pf";
        final String API_NAME = "regulatory_pf";
        final String S3_NAME = "trials_pf";
        final Regions AWS_REGION = Regions.US_EAST_1;

        final String AWS_ACCESS_KEY = "";
        final String AWS_SECRET_KEY = "";
        final String s3BucketName = "cs7319";
        final String s3BasePath = String.format("/base/%s", dataSource);
        final String s3ReadPath = String.format("base/%s/output.csv", dataSource);
        final String s3CuratedPath = String.format("/curated/%s", dataSource);
        final String s3SchemaPath = String.format("schema_log/%s/schema_definition.json", dataSource);
        final S3SchemaWriter s3SchemaWriter = new S3SchemaWriter(s3BucketName, s3SchemaPath, AWS_ACCESS_KEY, AWS_SECRET_KEY);

        // set up filters
        SchemaDefinitionFilter schemaDefinitionFilter = new SchemaDefinitionFilter(dataTypeInfererUtil, s3SchemaWriter);
        // S3Writer s3WriterBase = new S3Writer(s3BucketName, s3BasePath, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
        // S3Reader s3Reader = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, s3BucketName, s3ReadPath);
        // S3Writer s3WriterCurated = new S3Writer(s3BucketName, s3CuratedPath, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

        // // set up pipes
        // reader.setOut(pipe);
        // schemaDefinitionFilter.setIn(pipe);
        // s3WriterBase.setIn(pipe);

        //     // S3 specific
        // Pipe s3ReaderPipe = new Pipe(); 
        // s3Reader.setOut(s3ReaderPipe);
        // s3WriterCurated.setIn(s3ReaderPipe);

        // // set up threads
        // Thread readerThread = new Thread(reader);
        // Thread filterThread = new Thread(schemaDefinitionFilter);
        // Thread s3WriterBaseThread = new Thread(s3WriterBase);
        // Thread s3ReaderThread = new Thread(s3Reader);
        // Thread s3WriterCuratedThread = new Thread(s3WriterCurated);

        // readerThread.start();
        // filterThread.start();
        // s3WriterBaseThread.start();
        // s3ReaderThread.start();
        // s3WriterCuratedThread.start();

        // String loggerStringEnd = String.format("End of %s", dataSource);
        // MetaDataLogger.logMetaData(loggerStringEnd);

        // add in s3reader from base layer

        // add in s3writer to curated layer
    }
}
  

