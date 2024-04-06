package org.healthetl.filters;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ApiReader extends Filter {
    public void run(){
        callPatientsApi();
    }

    public JSONArray callPatientsApi() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/api/patients"))
                .method("GET", HttpRequest.BodyPublishers.noBody())
                .build();

        JSONArray jsonArray = new JSONArray();

        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                JSONParser parser = new JSONParser();
                JSONArray jsonArrayResponse = (JSONArray) parser.parse(response.body());

                for (Object obj : jsonArrayResponse) {
                    JSONObject jsonObject = (JSONObject) obj;
                    jsonArray.add(jsonObject);
                    // output.write(jsonObject);
                }
                //Thread.sleep(2000);
                // output.notifyThreads();

            } else {
                System.out.println("HTTP request failed with status code: " + response.statusCode());
            }
        } catch (IOException | InterruptedException | ParseException e) {
            System.out.println(e.getMessage());
        }
        return jsonArray;
    }
}
