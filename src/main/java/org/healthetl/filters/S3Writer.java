package org.healthetl.filters;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.json.simple.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class S3Writer extends Filter {

    private final String bucketName;
    private final AmazonS3 s3Client;

    public S3Writer(String bucketName, String accessKey, String secretKey, Regions region) {
        this.bucketName = bucketName;
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        this.s3Client = AmazonS3ClientBuilder.standard()
                                             .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                                             .withRegion(region)
                                             .build();
    }

    @Override
    public void run() {
        try {
            writeToS3(input.read());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void writeToS3(JSONObject json) {
        // System.out.println(json);
        // Convert JSON string to CSV
        String csvData = convertJsonToCsv(json);

        // Write CSV data to S3
        InputStream inputStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(csvData.getBytes(StandardCharsets.UTF_8).length);
        s3Client.putObject(new PutObjectRequest(bucketName, "output.csv", inputStream, metadata));
        System.out.println("Successfully wrote data to S3.");
    }

    private String convertJsonToCsv(JSONObject json) {
        StringBuilder csvBuilder = new StringBuilder();
    
        // Write header row with column names
        json.keySet().forEach(key -> csvBuilder.append('"').append(key).append('"').append(','));
        if (csvBuilder.length() > 0) {
            csvBuilder.deleteCharAt(csvBuilder.length() - 1); // Remove the last comma
        }
        csvBuilder.append('\n');
    
        // Write data rows
        csvBuilder.append(convertObjectToCsvLine(json));
        csvBuilder.append('\n');
    
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
