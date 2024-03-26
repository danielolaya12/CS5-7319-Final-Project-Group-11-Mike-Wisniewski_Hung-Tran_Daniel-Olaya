package org.healthetl;

import org.healthetl.connectors.Pipe;
import org.healthetl.filters.*;

public class Main {
    public static void main(String[] args) {
        Filter[] filters = new Filter[] {new ApiReader(), new CsvReader()};
//        setOut(filters);
        Pipe p = new Pipe();
        for(Filter filer: filters){
            filer.setOut(p);
        }
        DataTypeInferer inferer = new DataTypeInferer();
        inferer.setIn(p);
        startFilters(filters);
        Thread thread = new Thread(inferer);
        thread.start();
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