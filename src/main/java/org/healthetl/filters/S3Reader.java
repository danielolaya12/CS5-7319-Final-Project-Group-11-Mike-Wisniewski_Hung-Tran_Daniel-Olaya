package org.healthetl.filters;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.simple.JSONObject;

public class S3Reader extends Filter {

    private AmazonS3 s3Client;
    private final String bucketName;
    private final String key;

    public S3Reader(String accessKey, String secretKey, String bucketName, String key) {
        this.bucketName = bucketName;
        this.key = key;

        this.s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    @Override
    public void run() {
        try (S3Object s3Object = s3Client.getObject(bucketName, key);
             S3ObjectInputStream inputStream = s3Object.getObjectContent();
             Reader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
            for (CSVRecord csvRecord : csvParser) {
                JSONObject jsonObject = new JSONObject();
                csvParser.getHeaderNames().forEach(header -> jsonObject.put(header, csvRecord.get(header)));
                output.write(jsonObject);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
