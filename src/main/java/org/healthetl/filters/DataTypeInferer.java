package org.healthetl.filters;

import org.json.simple.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class DataTypeInferer extends Filter{
    ArrayList<String> jsonList = new ArrayList<>();
    String readerService;
    public DataTypeInferer(String readerService){
        this.readerService = readerService;
    }
    @Override
    public void run() {
        read();
        writeToFile();
    }

    public void read() {
        try {
            System.out.println("======== Reading =========" + readerService + "=========");
            JSONObject json;
            while((json = input.read()) != null){
                jsonList.add(json.toJSONString());
                System.out.println(readerService + "------" + json.toJSONString());
            }
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void writeToFile(){
        try {
            System.out.println("======== Filter  Text File =========" + readerService + "=========");
            FileWriter myWriter = new FileWriter(readerService + ".txt");
            for (String json: jsonList){
                myWriter.write(json + "\n");
            }
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
