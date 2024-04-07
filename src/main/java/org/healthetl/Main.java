package org.healthetl;

import com.amazonaws.regions.Regions;
import lombok.extern.log4j.Log4j2;
import org.healthetl.connectors.Pipe;
import org.healthetl.data.S3SchemaWriter;
import org.healthetl.filters.*;
import org.healthetl.utils.DataTypeInfererUtil;

@Log4j2
public class Main {
    
    private static final DataTypeInfererUtil dataTypeInfererUtil = new DataTypeInfererUtil();
    private static final Regions AWS_REGION = Regions.US_EAST_1;
    private static final String AWS_ACCESS_KEY = "AKIAV2RUR3AD4VVPWSZB";
    private static final String AWS_SECRET_KEY = "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";
    private static final String s3BucketName = "cs7319";
    public static void main(String[] args) {
        setupReader(new CsvReader(), new Pipe(), "patients_pf");
//        setupReader(new MSSQLReader(), new Pipe(), "medical_pf");
//        setupReader(new PostgresReader(), new Pipe(), "operations_pf");
//        setupReader(new S3Reader(AWS_ACCESS_KEY, AWS_SECRET_KEY, s3BucketName, "inbound/medications.csv"), new Pipe(), "trials_pf");
        setupReader(new ApiReader(), new Pipe(), "regulatory_pf");
    }
    
    private static void setupReader(Filter reader, Pipe pipe, String dataSource) {
        String loggerStringBegin = String.format("Start of %s", dataSource);
        log.info(loggerStringBegin);

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

        try {
            readerThread.start();
            filterThread.start();
            readerThread.join();
            filterThread.join();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }

        System.out.println("To Threads");

        try {
            s3WriterBaseThread.start();
            s3ReaderThread.start();
            s3WriterBaseThread.join();
            s3ReaderThread.join();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }

        s3WriterCuratedThread.start();

        String loggerStringEnd = String.format("End of %s", dataSource);
        log.info(loggerStringEnd);
    }
}
