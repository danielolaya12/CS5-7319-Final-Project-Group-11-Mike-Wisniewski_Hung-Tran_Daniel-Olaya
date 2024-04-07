package org.healthetl.filters;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Log4j2
public class S3Writer extends Filter {

    private final String bucketName;
    private final String pathName;
    private final AmazonS3 s3Client;

    public S3Writer(String bucketName, String pathName, String accessKey, String secretKey, Regions region) {
        this.bucketName = bucketName;
        this.pathName = pathName;
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        this.s3Client = AmazonS3ClientBuilder.standard()
                                             .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                                             .withRegion(region)
                                             .build();
    }

    @Override
    public void run() {

        try {
            List<JSONObject> jsonList = new ArrayList<>();
            JSONObject json;
            while (true) {
                json = input.read();
                jsonList.add(json);
                Integer nextValue = input.next();
                
                if (nextValue == 0) {
                    break; // Exit the loop if there's no more data
                }

            }

            writeToS3(concatenateToJsonArray(jsonList));

        } catch (InterruptedException e) {
            // log.error(e.getMessage());
        }
    }

    private JSONArray concatenateToJsonArray(List<JSONObject> jsonObjects) {
        JSONArray jsonArray = new JSONArray();
        for (JSONObject jsonObj : jsonObjects) {
            jsonArray.add(jsonObj);
        }
        return jsonArray;
    }

    public void writeToS3(JSONArray jsonArray) {
        // Convert JSON array to CSV
        String csvData = convertJsonArrayToCsv(jsonArray);
    
        // Write CSV data to S3
        InputStream inputStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(csvData.getBytes(StandardCharsets.UTF_8).length);
        String destinationLocation = bucketName + pathName;
        s3Client.putObject(new PutObjectRequest(destinationLocation, "output.csv", inputStream, metadata));
        System.out.println("Successfully wrote data to S3.");
    }

    private String convertJsonArrayToCsv(JSONArray jsonArray) {
        StringBuilder csvBuilder = new StringBuilder();
    
        // Write header row with column names
        JSONObject firstJsonObject = (JSONObject) jsonArray.get(0);
        firstJsonObject.keySet().forEach(key -> csvBuilder.append('"').append(key).append('"').append(','));
        if (csvBuilder.length() > 0) {
            csvBuilder.deleteCharAt(csvBuilder.length() - 1); // Remove the last comma
        }
        csvBuilder.append('\n');
    
        // Write data rows
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            csvBuilder.append(convertObjectToCsvLine(jsonObject)).append('\n');
        }
    
        return csvBuilder.toString();
    }
    
    private String convertObjectToCsvLine(JSONObject jsonObject) {
        StringBuilder lineBuilder = new StringBuilder();
    
        jsonObject.keySet().forEach(key -> lineBuilder.append('"').append(jsonObject.get(key)).append('"').append(','));
        if (lineBuilder.length() > 0) {
            lineBuilder.deleteCharAt(lineBuilder.length() - 1); // Remove the last comma
        }
    
        return lineBuilder.toString();
    }
}
