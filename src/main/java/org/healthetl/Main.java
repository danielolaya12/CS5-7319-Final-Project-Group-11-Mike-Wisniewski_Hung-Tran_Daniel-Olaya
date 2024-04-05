package org.healthetl;

import com.amazonaws.regions.Regions;
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
        String BUCKETNAME_MSSQL_BASE = "cs7319/base/medical";
        String BUCKETNAME_MSSQL_CURATED = "cs7319/curated/medical";
        String BUCKETNAME_POSTGRES_BASE = "cs7319/base/operations";
        String BUCKETNAME_POSTGRES_CURATED = "cs7319/curated/operations";
        String BUCKETNAME_CSV_BASE = "cs7319/base/patient";
        String BUCKETNAME_CSV_CURATED = "cs7319/curated/patient";
        String BUCKETNAME_API_BASE = "cs7319/base/regulatory";
        String BUCKETNAME_API_CURATED = "cs7319/curated/regulatory";
        String BUCKETNAME_S3_BASE = "cs7319/base/trials";
        String BUCKETNAME_S3_CURATED = "cs7319/curated/trials";
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
        MSSQLReader mssqlPipeline = new MSSQLReader();
        S3Writer s3WriterMSSQL_base = new S3Writer(BUCKETNAME_MSSQL_BASE, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
        S3Reader s3ReaderMSSQL_base = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKETNAME_MSSQL_BASE, "output.csv");
        S3Writer s3WriterMSSQL_curated = new S3Writer(BUCKETNAME_MSSQL_CURATED, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

        PostgresReader postgresPipeline = new PostgresReader();
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
        s3WriterMSSQLBaseThread.start();
        s3ReaderMSSQLThread.start();
        s3WriterMSSQLCuratedThread.start();
        
        // wait for both threads to finish (if needed)
        try {

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
