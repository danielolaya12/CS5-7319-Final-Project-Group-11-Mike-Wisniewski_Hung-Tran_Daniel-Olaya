package org.healthetl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import javax.xml.crypto.Data;

import org.healthetl.connectors.C2;
import org.healthetl.connectors.Pipe;
import org.healthetl.filters.*;
import org.json.simple.JSONObject;

import com.amazonaws.regions.Regions;


public class Main {
    public static void main(String[] args) {
        // //Setup API reader
        // ApiReader apiReader = new ApiReader();
        // Pipe apiPipe = new Pipe();
        // DataTypeInferer apiInferer = new DataTypeInferer("ApiData");
        // apiReader.setOut(apiPipe);
        // apiInferer.setIn(apiPipe);
        // Thread t1 = new Thread(apiReader);
        // Thread t2 = new Thread(apiInferer);
        // t1.start();
        // t2.start();

        // S3Reader s3Reader = new S3Reader("", "", "7319-software-architecture", "healthcare_dataset.csv");
        // Pipe s3Pipe = new Pipe();
        // DataTypeInferer s3Inferer = new DataTypeInferer("s3Data");
        // s3Reader.setOut(s3Pipe);
        // s3Inferer.setIn(s3Pipe);
        // Thread t5 = new Thread(s3Reader);
        // Thread t6 = new Thread(s3Inferer);
        // t5.start();
        // t6.start();

        // //Meta Data Logger example
        // MetaDataLogger.logMetaData("Start C2");

        // // Setup MSSQL Pipe and Filter
        // C2 mainC2 = new C2();

        // // downstream messaging
        // String downstreamMessage = mainC2.downstreamMessage("Start API");
        
        // // upstream messaging
        // String upstreamMessage = mainC2.upstreamMessage(downstreamMessage);

        // System.out.println("All threads have completed");

        // // S3 Writer
        // String bucketName = "cs7319/curated/medical/data.csv";
        // String accessKey = "";
        // String secretKey = "";
        // Regions region = Regions.US_EAST_1;
        // S3Writer s3Writer = new S3Writer(bucketName, accessKey, secretKey, region);
        // // Create a mock JSONObject
        // JSONObject jsonData = new JSONObject();
        // jsonData.put("name", "John");
        // jsonData.put("age", 30);
        // jsonData.put("city", "New York");
        // // JSONObject jsonData =
        // s3Writer.writeToS3(jsonData);

        // static "global" variables
        String BUCKETNAME_MSSQL_BASE = "cs7319/base/medical_pf";
        String BUCKETNAME_MSSQL_CURATED = "cs7319/curated/medical_pf";
        String BUCKETNAME_POSTGRES_BASE = "cs7319/base/operations_pf";
        String BUCKETNAME_POSTGRES_CURATED = "cs7319/curated/operations_pf";
        String BUCKETNAME_CSV_BASE = "cs7319/base/patient_pf";
        String BUCKETNAME_CSV_CURATED = "cs7319/curated/patient_pf";
        String BUCKETNAME_API_BASE = "cs7319/base/regulatory_pf";
        String BUCKETNAME_API_CURATED = "cs7319/curated/regulatory_pf";
        String BUCKETNAME_S3_BASE = "cs7319/base/trials_pf";
        String BUCKETNAME_S3_CURATED = "cs7319/curated/trials_pf";
        String AWS_ACCESS_KEY = "";
        String AWS_SECRET_KEY = "";
        Regions AWS_REGION = Regions.US_EAST_1;

        // create pipes
        Pipe inputMSSQLPipe = new Pipe();
        Pipe outputMSSQLPipe = new Pipe();
        Pipe inputPostgresPipe = new Pipe();
        Pipe outputPostgresPipe = new Pipe();
        Pipe inputCsvPipe = new Pipe();
        Pipe outputCsvPipe = new Pipe();
        Pipe outputS3ReaderPipe = new Pipe();
        
        // create filter instances
        MSSQLPipeline mssqlPipeline = new MSSQLPipeline();
        SchemaDefinition mssqlSchemaDefinition = new SchemaDefinition("cs7319", "schema_log/medical_pf/schema_definition.json", AWS_ACCESS_KEY, AWS_SECRET_KEY);
        S3Writer s3WriterMSSQL_base = new S3Writer(BUCKETNAME_MSSQL_BASE, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
        S3Reader s3ReaderMSSQL_base = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKETNAME_MSSQL_BASE, "output.csv");
        S3Writer s3WriterMSSQL_curated = new S3Writer(BUCKETNAME_MSSQL_CURATED, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

        PostgresPipeline postgresPipeline = new PostgresPipeline();
        S3Writer s3WriterPostgres_base = new S3Writer(BUCKETNAME_POSTGRES_BASE, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
        S3Reader s3ReaderPostgres_base = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKETNAME_POSTGRES_BASE, "output.csv");
        S3Writer s3WriterPostgres_curated = new S3Writer(BUCKETNAME_POSTGRES_CURATED, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

        CsvReader csvReader = new CsvReader();
        S3Writer s3WriterCsv_base = new S3Writer(BUCKETNAME_CSV_BASE, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
        S3Reader s3ReaderCsv_base = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKETNAME_CSV_BASE, "output.csv");
        S3Writer s3WriterCsv_curated = new S3Writer(BUCKETNAME_CSV_CURATED, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

        
        // set in and out pipes
        mssqlPipeline.setIn(inputMSSQLPipe);
        mssqlPipeline.setOut(outputMSSQLPipe);
        mssqlSchemaDefinition.setIn(outputMSSQLPipe);
        s3WriterMSSQL_base.setIn(outputMSSQLPipe);
        s3ReaderMSSQL_base.setOut(outputS3ReaderPipe);
        s3WriterMSSQL_curated.setIn(outputS3ReaderPipe);

        postgresPipeline.setIn(inputPostgresPipe);
        postgresPipeline.setOut(outputPostgresPipe);
        s3WriterPostgres_base.setIn(outputPostgresPipe);
        s3ReaderPostgres_base.setOut(outputS3ReaderPipe);
        s3WriterPostgres_curated.setIn(outputS3ReaderPipe);

        csvReader.setIn(inputCsvPipe);
        csvReader.setOut(outputCsvPipe);
        s3WriterCsv_base.setIn(outputCsvPipe);
        s3ReaderCsv_base.setOut(outputS3ReaderPipe);
        s3WriterCsv_curated.setIn(outputS3ReaderPipe);
        
        // create threads
        Thread mssqlThread = new Thread(mssqlPipeline);
        Thread mssqlSchemaDefinitionThread = new Thread(mssqlSchemaDefinition);
        Thread s3WriterMSSQLBaseThread = new Thread(s3WriterMSSQL_base);
        Thread s3ReaderMSSQLThread = new Thread(s3ReaderMSSQL_base);
        Thread s3WriterMSSQLCuratedThread = new Thread(s3WriterMSSQL_curated);

        Thread postgresThread = new Thread(postgresPipeline);
        Thread s3WriterPostgresBaseThread = new Thread(s3WriterPostgres_base);
        Thread s3ReaderPostgresThread = new Thread(s3ReaderPostgres_base);
        Thread s3WriterPostgresCuratedThread = new Thread(s3WriterPostgres_curated);

        Thread csvThread = new Thread(csvReader);
        Thread s3WriterCsvBaseThread = new Thread(s3WriterCsv_base);
        Thread s3ReaderCsvThread = new Thread(s3ReaderCsv_base);
        Thread s3WriterCsvCuratedThread = new Thread(s3WriterCsv_curated);
        
        // start source to base layer threads
        mssqlThread.start();
        mssqlSchemaDefinitionThread.start();
        s3WriterMSSQLBaseThread.start();
        s3ReaderMSSQLThread.start();
        s3WriterMSSQLCuratedThread.start();
        
        // wait for both threads to finish (if needed)
        try {

            // log schema
            // mssqlThread.join();
            // mssqlSchemaDefinitionThread.join();

            // base layer
            mssqlThread.join();
            s3WriterMSSQLBaseThread.join();

            // curated layer
            s3ReaderMSSQLThread.join();
            s3WriterMSSQLCuratedThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // start base to curated layer threads
        postgresThread.start();
        s3WriterPostgresBaseThread.start();
        s3ReaderPostgresThread.start();
        s3WriterPostgresCuratedThread.start();

        // wait for both threads to finish
        try {
            postgresThread.join();
            s3WriterPostgresBaseThread.join();

            s3ReaderPostgresThread.join();
            s3WriterPostgresCuratedThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // start base to curated layer threads
        csvThread.start();
        s3WriterCsvBaseThread.start();
        s3ReaderCsvThread.start();
        s3WriterCsvCuratedThread.start();

        // wait for both threads to finish
        try {
            csvThread.join();
            s3WriterCsvBaseThread.join();

            s3ReaderCsvThread.join();
            s3WriterCsvCuratedThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //Set single output
    public static void setOut(Filter[] filters){
        Pipe p = new Pipe();
        for(Filter filer: filters){
            filer.setOut(p);
        }
    }
    private static void startFilters(Runnable[] filters) {
        for (Runnable filter : filters) {
            Thread thread = new Thread(filter);
            thread.start();
        }
    }

    public static void connectFilters(Filter[] filters) {
        for (int i = 0; i < filters.length - 1; i++) {
            Pipe p = new Pipe();
            filters[i].setOut(p);
            filters[i + 1].setIn(p);
        }
    }
}
