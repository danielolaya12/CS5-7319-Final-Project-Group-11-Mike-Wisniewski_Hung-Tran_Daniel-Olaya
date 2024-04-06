package org.healthetl.filters;

import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Log4j2
public class ApiReader extends Filter {
    public void run(){
        callPatientsApi();
    }

    public void callPatientsApi() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/api/patients"))
                .method("GET", HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                JSONParser parser = new JSONParser();
                JSONArray jsonArray = (JSONArray) parser.parse(response.body());

                for (Object obj : jsonArray) {
                    JSONObject jsonObject = (JSONObject) obj;
                    output.write(jsonObject);
                }
                output.notifyThreads();
            } else {
                log.error("HTTP request failed with status code: " + response.statusCode());
            }
        } catch (IOException | InterruptedException | ParseException e) {
            log.error(e.getMessage());
        }
    }
}
