package org.healthetl.filters;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class S3Writer {

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

    public void writeToS3(String json) {
        try {
            // Convert JSON string to CSV
            String csvData = convertJsonToCsv(json);

            // Write CSV data to S3
            InputStream inputStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(csvData.getBytes(StandardCharsets.UTF_8).length);
            s3Client.putObject(new PutObjectRequest(bucketName, "output.csv", inputStream, metadata));
            System.out.println("Successfully wrote data to S3.");
        } catch (ParseException e) {
            System.err.println("Error writing to S3: " + e.getMessage());
        }
    }

    private String convertJsonToCsv(String json) throws ParseException {
        StringBuilder csvBuilder = new StringBuilder();

        JSONParser parser = new JSONParser();
        JSONArray jsonArray = (JSONArray) parser.parse(json);
        if (!jsonArray.isEmpty()) {
            // Get column names from the first JSON object
            JSONObject firstObject = (JSONObject) jsonArray.get(0);
            firstObject.keySet().forEach(key -> csvBuilder.append('"').append(key).append('"').append(','));

            // Remove the last comma
            csvBuilder.deleteCharAt(csvBuilder.length() - 1);
            csvBuilder.append('\n');

            // Write data rows
            for (Object obj : jsonArray) {
                JSONObject jsonObject = (JSONObject) obj;
                jsonObject.keySet().forEach(key -> csvBuilder.append('"').append(jsonObject.get(key)).append('"').append(','));
                csvBuilder.deleteCharAt(csvBuilder.length() - 1);
                csvBuilder.append('\n');
            }
        }

        return csvBuilder.toString();
    }


    public static void main(String[] args) {
        String jsonInput = "[{\"name\":\"John\",\"age\":30,\"city\":\"New York\"},{\"name\":\"Alice\",\"age\":25,\"city\":\"Los Angeles\"}]";
        String bucketName = "cs7319/base/medical/data.csv";
        // String s3SchemaPath = "schema_log/schema_definition.json";
        String accessKey = "";
        String secretKey = "";
        Regions region = Regions.US_EAST_1; // Change this to your desired region

        S3Writer s3Writer = new S3Writer(bucketName, accessKey, secretKey, region);
        s3Writer.writeToS3(jsonInput);
    }
}
