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
