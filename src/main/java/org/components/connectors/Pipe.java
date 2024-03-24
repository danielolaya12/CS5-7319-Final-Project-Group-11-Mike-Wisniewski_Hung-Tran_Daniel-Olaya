package org.components.connectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Pipe {
    private JSONArray jsonArray = new JSONArray();
    public synchronized void write(JSONObject jsonObject) {
        jsonArray.add(jsonObject);
        notify();
    }

    public synchronized String read() throws InterruptedException {
        while (jsonArray.isEmpty()) {
            wait();
        }
        return jsonArray.removeFirst().toString();
    }
}