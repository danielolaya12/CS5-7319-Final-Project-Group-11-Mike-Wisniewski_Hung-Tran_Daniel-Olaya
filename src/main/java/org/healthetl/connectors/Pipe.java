package org.healthetl.connectors;

import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONObject;

import java.util.LinkedList;

@Log4j2
public class Pipe {
    private LinkedList<JSONObject> jsonArray = new LinkedList<>();
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

    public synchronized Integer next() {
        Integer startFlag = 1;
        while (jsonArray.size() != 1 | jsonArray.size() != 0) {
            startFlag = jsonArray.size();
            return jsonArray.size();
        }

        // System.out.println(startFlag);
        while (jsonArray.size() == 0) {
            if (startFlag == 0) {
                return 0;
            } else {
                return jsonArray.size();
            }
        }

        return 1;
    }

    public synchronized void notifyThreads(){
        try {
            if (Thread.holdsLock(this)) {
                notify();
            } else {
                System.out.println("Current thread does not own the lock on the Pipe object.");
            }
        } catch (Exception e){
            // log.error(e.getMessage());
        }
    }
}
