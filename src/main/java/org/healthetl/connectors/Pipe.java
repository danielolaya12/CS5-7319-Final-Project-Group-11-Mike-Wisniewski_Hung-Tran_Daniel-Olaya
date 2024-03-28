package org.healthetl.connectors;

import org.json.simple.JSONObject;

import java.util.LinkedList;

public class Pipe {
    LinkedList<JSONObject> jsonArray = new LinkedList<>();
    public synchronized void write(JSONObject jsonObject) {
        jsonArray.add(jsonObject);
        notify();
    }

    public synchronized JSONObject read() throws InterruptedException {
        while (jsonArray.isEmpty()) {
            wait();
        }
        return jsonArray.removeFirst();
    }

    public synchronized void notifyThreads(){
        try {
            if (Thread.holdsLock(this)) {
                notify();
            } else {
                System.out.println("Current thread does not own the lock on the Pipe object.");
            }
        } catch (Exception e){

        }
    }
}

// C2Connector_Upstream (String message){
//     if (message = "stop message for API"):
//             parquetWriter(jsonObject);
//             metaDataLogger();
//             

//     else (message = "stop message for CSV"):
//             parquetWriter(jsonObject);
//             metaDataLogger();
//             
// }

// public class C2Connector_Downstream(String message) {
//     private JSONArray jsonArray = new JSONArray();
//     public synchronized void write(JSONObject jsonObject) {
//         if (message = "Start API"):
//             jsonObject = ApiReader();
//             schemaLog(jsonObject);
//             DataTypeInfer(jsonObject);
    //  Message API_msg = "API is Done"         
        // return API_msg

//         else (message = "This is another message")
//             CsvReader;
//         jsonArray.add(jsonObject);
//         notify();
//     }