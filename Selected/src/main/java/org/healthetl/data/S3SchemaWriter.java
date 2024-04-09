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
    private final String KEYID;
    private final String SECRETKEY;

    public S3SchemaWriter(String s3BucketName, String s3SchemaPath, String KEYID, String SECRETKEY) {
        this.s3BucketName = s3BucketName;
        this.s3SchemaPath = s3SchemaPath;
        this.KEYID = KEYID;
        this.SECRETKEY = SECRETKEY;
    }
    public void writeJsonToS3(JSONObject schemaDefinition) {
        // set credentials
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(KEYID, SECRETKEY);

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
