package org.healthetl.data;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.json.simple.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

public class S3SchemaWriter {
    private final String s3BucketName;
    private final String s3SchemaPath;

    public S3SchemaWriter(String s3BucketName, String s3SchemaPath) {
        this.s3BucketName = s3BucketName;
        this.s3SchemaPath = s3SchemaPath;
    }
    public void writeJsonToS3(JSONObject schemaDefinition, String readerService) {
        // AWS creds
        // TODO: change to windows path
        String kid = "AKIAV2RUR3AD4VVPWSZB";
        String sak = "dR/rfjYnbKZjIJy2VFxnpMyCGG9wDx6uv+ROFohg";

        // set credentials
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(kid, sak);

        //================================================================
        /*
        System.out.println("Into the S3 Writer ===== ");
        System.out.println(schemaDefinition);
        System.out.println("======================== ");

        String filename = readerService + ".txt";
        try(FileWriter myWriter = new FileWriter(filename)) {
            myWriter.write(schemaDefinition + "\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Finished writing to txt");

         */
        //================================================================

        // initialize aws access
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

        // convert json object to streamable bytes and store in s3
        String jsonString = schemaDefinition.toJSONString();
        byte[] contentBytes = jsonString.getBytes();
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        s3Client.putObject(new PutObjectRequest(s3BucketName, s3SchemaPath, inputStream, metadata));
    }
}
