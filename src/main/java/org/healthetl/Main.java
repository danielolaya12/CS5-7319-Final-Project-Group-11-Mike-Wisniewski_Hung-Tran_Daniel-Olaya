package org.healthetl;

import org.healthetl.connectors.Pipe;
import org.healthetl.filters.*;

import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        //Setup API reader
        ApiReader apiReader = new ApiReader();
        Pipe apiPipe = new Pipe();
        DataTypeInferer apiInferer = new DataTypeInferer("ApiData");
        apiReader.setOut(apiPipe);
        apiInferer.setIn(apiPipe);
        Thread t1 = new Thread(apiReader);
        t1.start();
        Thread t2 = new Thread(apiInferer);
        t2.start();

        //Setup CSV reader
        CsvReader csvReader = new CsvReader();
        Pipe csvPipe = new Pipe();
        DataTypeInferer csvInferer = new DataTypeInferer("csvData");
        csvReader.setOut(csvPipe);
        csvInferer.setIn(csvPipe);
        Thread t3 = new Thread(csvReader);
        t3.start();
        Thread t4 = new Thread(csvInferer);
        t4.start();

        // Wait for all threads to complete
        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Once all threads have completed, you can continue with any additional processing
        System.out.println("All threads have completed. Continuing with further processing...");
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