package org.components.filters;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class CsvReader extends Filter{

    public void run (){
        readInput();
    }
    private static void readInput() {
        JSONArray jsonArray = new JSONArray();
        try (Reader reader = new FileReader("Independent_Medical_Reviews.csv");
             CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord csvRecord : csvParser) {
                JSONObject jsonObject = new JSONObject();
                for (String header : csvParser.getHeaderNames()) {
                    jsonObject.put(header, csvRecord.get(header));
                }
                jsonArray.add(jsonObject);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(jsonArray.toJSONString());
    }
}
