package org.healthetl;

import com.amazonaws.regions.Regions;
import org.healthetl.connectors.Pipe;
import org.healthetl.data.S3SchemaWriter;
import org.healthetl.filters.*;
import org.healthetl.utils.DataTypeInfererUtil;

public class Main {
    
    private static final DataTypeInfererUtil dataTypeInfererUtil = new DataTypeInfererUtil();
    
    public static void main(String[] args) {
        String AWS_ACCESS_KEY = "AKIAV2RUR3AD4VVPWSZB";
        String AWS_SECRET_KEY = "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";
        String s3BucketName = "cs7319";

        setupReader(new CsvReader(), new Pipe(), "patients_pf");
        setupReader(new MSSQLReader(), new Pipe(), "medical_pf");
        setupReader(new PostgresReader(), new Pipe(), "operations_pf");
        setupReader(new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, s3BucketName, "inbound/medications.csv"), new Pipe(), "trials_pf");
        setupReader(new ApiReader(), new Pipe(), "regulatory_pf");

        /*
        //Meta Data Logger example
        MetaDataLogger.logMetaData("Start C2");
         */
        

    }
    
    private static void setupReader(Filter reader, Pipe pipe, String dataSource) {

        // static "global" variables
        final String BUCKETNAME_MSSQL_BASE = "medical_pf";
        final String BUCKETNAME_POSTGRES_CURATED = "operations_pf";
        final String CSV_NAME = "patient_pf";
        final String API_NAME = "regulatory_pf";
        final String S3_NAME = "trials_pf";
        final Regions AWS_REGION = Regions.US_EAST_1;

        final String AWS_ACCESS_KEY = "AKIAV2RUR3AD4VVPWSZB";
        final String AWS_SECRET_KEY = "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";
        final String s3BucketName = "cs7319";
        final String s3BasePath = String.format("/base/%s", dataSource);
        final String s3ReadPath = String.format("base/%s/output.csv", dataSource);
        final String s3CuratedPath = String.format("/curated/%s", dataSource);
        final String s3SchemaPath = String.format("schema_log/%s/schema_definition.json", dataSource);
        final S3SchemaWriter s3SchemaWriter = new S3SchemaWriter(s3BucketName, s3SchemaPath, AWS_ACCESS_KEY, AWS_SECRET_KEY);

        // set up filters
        SchemaDefinitionFilter schemaDefinitionFilter = new SchemaDefinitionFilter(dataTypeInfererUtil, s3SchemaWriter);
        S3Writer s3WriterBase = new S3Writer(s3BucketName, s3BasePath, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
        S3Reader s3Reader = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, s3BucketName, s3ReadPath);
        S3Writer s3WriterCurated = new S3Writer(s3BucketName, s3CuratedPath, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

        // set up pipes
        reader.setOut(pipe);
        schemaDefinitionFilter.setIn(pipe);
        s3WriterBase.setIn(pipe);

            // S3 specific
        Pipe s3ReaderPipe = new Pipe(); 
        s3Reader.setOut(s3ReaderPipe);
        s3WriterCurated.setIn(s3ReaderPipe);

        // set up threads
        Thread readerThread = new Thread(reader);
        Thread filterThread = new Thread(schemaDefinitionFilter);
        Thread s3WriterBaseThread = new Thread(s3WriterBase);
        Thread s3ReaderThread = new Thread(s3Reader);
        Thread s3WriterCuratedThread = new Thread(s3WriterCurated);

        readerThread.start();
        filterThread.start();
        s3WriterBaseThread.start();
        s3ReaderThread.start();
        s3WriterCuratedThread.start();

        // add in s3reader from base layer

        // add in s3writer to curated layer
    }
}
    

    //     // create pipes
    //     Pipe inputMSSQLPipe = new Pipe();
    //     Pipe outputMSSQLPipe = new Pipe();
    //     Pipe inputPostgresPipe = new Pipe();
    //     Pipe outputPostgresPipe = new Pipe();
    //     Pipe inputCsvPipe = new Pipe();
    //     Pipe outputCsvPipe = new Pipe();
    //     Pipe outputS3ReaderPipe = new Pipe();
        
    //     // create filter instances
    //     MSSQLReader mssqlPipeline = new MSSQLReader();
    //     S3Writer s3WriterMSSQL_base = new S3Writer(BUCKETNAME_MSSQL_BASE, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
    //     S3Reader s3ReaderMSSQL_base = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKETNAME_MSSQL_BASE, "output.csv");
    //     S3Writer s3WriterMSSQL_curated = new S3Writer(BUCKETNAME_MSSQL_CURATED, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

    //     PostgresReader postgresPipeline = new PostgresReader();
    //     S3Writer s3WriterPostgres_base = new S3Writer(BUCKETNAME_POSTGRES_BASE, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
    //     S3Reader s3ReaderPostgres_base = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKETNAME_POSTGRES_BASE, "output.csv");
    //     S3Writer s3WriterPostgres_curated = new S3Writer(BUCKETNAME_POSTGRES_CURATED, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

    //     CsvReader csvReader = new CsvReader();
    //     S3Writer s3WriterCsv_base = new S3Writer(BUCKETNAME_CSV_BASE, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);
    //     S3Reader s3ReaderCsv_base = new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKETNAME_CSV_BASE, "output.csv");
    //     S3Writer s3WriterCsv_curated = new S3Writer(BUCKETNAME_CSV_CURATED, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION);

        
    //     // set in and out pipes
    //     mssqlPipeline.setIn(inputMSSQLPipe);
    //     mssqlPipeline.setOut(outputMSSQLPipe);
    //     // mssqlSchemaDefinition.setIn(outputMSSQLPipe);
    //     s3WriterMSSQL_base.setIn(outputMSSQLPipe);
    //     s3ReaderMSSQL_base.setOut(outputS3ReaderPipe);
    //     s3WriterMSSQL_curated.setIn(outputS3ReaderPipe);

    //     postgresPipeline.setIn(inputPostgresPipe);
    //     postgresPipeline.setOut(outputPostgresPipe);
    //     s3WriterPostgres_base.setIn(outputPostgresPipe);
    //     s3ReaderPostgres_base.setOut(outputS3ReaderPipe);
    //     s3WriterPostgres_curated.setIn(outputS3ReaderPipe);

    //     csvReader.setIn(inputCsvPipe);
    //     csvReader.setOut(outputCsvPipe);
    //     s3WriterCsv_base.setIn(outputCsvPipe);
    //     s3ReaderCsv_base.setOut(outputS3ReaderPipe);
    //     s3WriterCsv_curated.setIn(outputS3ReaderPipe);
        
    //     // create threads
    //     Thread mssqlThread = new Thread(mssqlPipeline);
    //     // Thread mssqlSchemaDefinitionThread = new Thread(mssqlSchemaDefinition);
    //     Thread s3WriterMSSQLBaseThread = new Thread(s3WriterMSSQL_base);
    //     Thread s3ReaderMSSQLThread = new Thread(s3ReaderMSSQL_base);
    //     Thread s3WriterMSSQLCuratedThread = new Thread(s3WriterMSSQL_curated);

    //     Thread postgresThread = new Thread(postgresPipeline);
    //     Thread s3WriterPostgresBaseThread = new Thread(s3WriterPostgres_base);
    //     Thread s3ReaderPostgresThread = new Thread(s3ReaderPostgres_base);
    //     Thread s3WriterPostgresCuratedThread = new Thread(s3WriterPostgres_curated);

    //     Thread csvThread = new Thread(csvReader);
    //     Thread s3WriterCsvBaseThread = new Thread(s3WriterCsv_base);
    //     Thread s3ReaderCsvThread = new Thread(s3ReaderCsv_base);
    //     Thread s3WriterCsvCuratedThread = new Thread(s3WriterCsv_curated);
        
    //     // start source to base layer threads
    //     mssqlThread.start();
    //     // mssqlSchemaDefinitionThread.start();
    //     s3WriterMSSQLBaseThread.start();
    //     s3ReaderMSSQLThread.start();
    //     s3WriterMSSQLCuratedThread.start();
        
    //     // wait for both threads to finish (if needed)
    //     try {

    //         // base layer
    //         mssqlThread.join();
    //         s3WriterMSSQLBaseThread.join();

    //         // curated layer
    //         s3ReaderMSSQLThread.join();
    //         s3WriterMSSQLCuratedThread.join();
    //     } catch (InterruptedException e) {
    //         e.printStackTrace();
    //     }

    //     // start base to curated layer threads
    //     postgresThread.start();
    //     s3WriterPostgresBaseThread.start();
    //     s3ReaderPostgresThread.start();
    //     s3WriterPostgresCuratedThread.start();

    //     // wait for both threads to finish
    //     try {
    //         postgresThread.join();
    //         s3WriterPostgresBaseThread.join();

    //         s3ReaderPostgresThread.join();
    //         s3WriterPostgresCuratedThread.join();
    //     } catch (InterruptedException e) {
    //         e.printStackTrace();
    //     }

    //     // start base to curated layer threads
    //     csvThread.start();
    //     s3WriterCsvBaseThread.start();
    //     s3ReaderCsvThread.start();
    //     s3WriterCsvCuratedThread.start();

    //     // wait for both threads to finish
    //     try {
    //         csvThread.join();
    //         s3WriterCsvBaseThread.join();

    //         s3ReaderCsvThread.join();
    //         s3WriterCsvCuratedThread.join();
    //     } catch (InterruptedException e) {
    //         e.printStackTrace();
    //     }
    // }

