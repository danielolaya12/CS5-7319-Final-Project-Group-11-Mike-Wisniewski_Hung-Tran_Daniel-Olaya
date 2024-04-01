package org.healthetl;

import javax.xml.crypto.Data;

import org.healthetl.connectors.C2;
import org.healthetl.connectors.Pipe;
import org.healthetl.filters.*;


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

        // //Setup CSV reader
        // CsvReader csvReader = new CsvReader();
        // Pipe csvPipe = new Pipe();
        // DataTypeInferer csvInferer = new DataTypeInferer("csvData");
        // csvReader.setOut(csvPipe);
        // csvInferer.setIn(csvPipe);
        // Thread t3 = new Thread(csvReader);
        // Thread t4 = new Thread(csvInferer);
        // t3.start();
        // t4.start();

        // S3Reader s3Reader = new S3Reader("", "", "7319-software-architecture", "healthcare_dataset.csv");
        // Pipe s3Pipe = new Pipe();
        // DataTypeInferer s3Inferer = new DataTypeInferer("s3Data");
        // s3Reader.setOut(s3Pipe);
        // s3Inferer.setIn(s3Pipe);
        // Thread t5 = new Thread(s3Reader);
        // Thread t6 = new Thread(s3Inferer);
        // t5.start();
        // t6.start();

        // // Setup MSSQL reader
        // MSSQLPipeline mssqlPipeline = new MSSQLPipeline();
        // Pipe mssqlPipe = new Pipe();
        // DataTypeInferer mssqlInferer = new DataTypeInferer("MSSQL");
        // mssqlPipeline.setOut(mssqlPipe);
        // mssqlInferer.setIn(mssqlPipe);
        // Thread t7 = new Thread(mssqlPipeline);
        // Thread t8 = new Thread(mssqlInferer);
        // t7.start();
        // t8.start();

        // // Setup PostGres reader
        // PostgresPipeline postgresPipeline = new PostgresPipeline();
        // Pipe postgresPipe = new Pipe();
        // DataTypeInferer postgresInferer = new DataTypeInferer("Postgres");
        // postgresPipeline.setOut(postgresPipe);
        // postgresInferer.setIn(postgresPipe);
        // Thread t9 = new Thread(postgresPipeline);
        // Thread t10 = new Thread(postgresInferer);
        // t9.start();
        // t10.start();

        // Setup MSSQL Pipe and Filter
        C2 mssqlC2 = new C2();

        // downstream messaging
        String downstreamMessage = mssqlC2.downstreamMessage("Start API");
        
        // upstream messaging
        String upstreamMessage = mssqlC2.upstreamMessage(downstreamMessage);

        System.out.println("All threads have completed");
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