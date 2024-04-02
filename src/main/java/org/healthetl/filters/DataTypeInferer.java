package org.healthetl.filters;

import org.json.simple.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class DataTypeInferer extends Filter{
    private final ArrayList<String> jsonList = new ArrayList<>();
    private final String readerService;
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
            String filename = readerService + ".txt";
            try(FileWriter myWriter = new FileWriter(filename)) {
                while ((json = input.read()) != null) {
                    myWriter.write(json + "\n");
                }
            }
            System.out.println("Finished writing to CSV");
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void writeToFile(){
        String filename = readerService + ".txt";
        try(FileWriter myWriter = new FileWriter(filename)){
            synchronized (jsonList){
                for(String word : jsonList) {
                    myWriter.write(word + "\n");
                }
            }
        } catch( IOException e ) {
            System.out.println("Error writing to file");
        }
    }
}
