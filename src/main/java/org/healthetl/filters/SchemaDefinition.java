package org.healthetl.filters;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.Map;
import java.util.Set;


public class SchemaDefinition extends Filter {

    private final String s3BucketName;
    private final String s3SchemaPath;
    private final String KEYID;
    private final String SECRETKEY;

    public SchemaDefinition(String s3BucketName, String s3SchemaPath, String KEYID, String SECRETKEY) {
        this.s3BucketName = s3BucketName;
        this.s3SchemaPath = s3SchemaPath;
        this.KEYID = KEYID;
        this.SECRETKEY = SECRETKEY;
    }

    @Override
    public void run() {
        try {
            schemaLog(input.read().toString(), s3BucketName, s3SchemaPath, KEYID, SECRETKEY);
        } catch (InterruptedException | ParseException | IOException e) {
            e.printStackTrace();
        }
    }

    // Method to infer data types and write the result to an S3 bucket
    public static void schemaLog(String jsonInput, String s3BucketName, String s3SchemaPath, String KEYID, String SECRETKEY) throws ParseException, IOException {

        // create schema definition
        JSONObject schemaDefinition = inferDataTypes(jsonInput);

        System.out.println(schemaDefinition);

        // output to s3
        writeJsonToS3(schemaDefinition, s3BucketName, s3SchemaPath, KEYID, SECRETKEY);
    }

    private static JSONObject inferDataTypes(String jsonInput) {
        JSONObject result = new JSONObject();
    
        try {
            // Parse the JSON input String into a JSONObject
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(jsonInput);
    
            // Iterate over all entries in the JSON object
            for (Object obj : jsonObject.entrySet()) {
                Map.Entry<String, Object> entry = (Map.Entry<String, Object>) obj;
                String key = entry.getKey();
                Object value = entry.getValue();
    
                // Infer the data type of the value
                Class<?> valueType = inferValueType(value);
    
                // Put the inferred data type into the result JSON object
                result.put(key, valueType != null ? valueType.getSimpleName() : "Unknown");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    
        return result;
    }

    private static Class<?> inferValueType(Object value) {
        
        // string types
        if (value instanceof String) {
            
            // cast value as string
            String stringValue = (String) value;

            // if date
            if (isDate(stringValue)) {
                return Date.class;
            } 
            
            // if timestamp
            else if (isTimestamp(stringValue)) {
                return Timestamp.class;
            } 
            
            // if string
            else {
                return String.class;
            }
        } 

        // if number
        else if (value instanceof String) {
            String strValue = (String) value;

            // if int
            if (isInteger(strValue)) {
                return Long.class;
            } 
            
            // if float/double
            else if (isDouble(strValue)) {
                return Double.class;
            }
        } 

        // if bool
        else if (value instanceof Boolean) {
            return Boolean.class;
        }
        return null;
    }

    private static void writeJsonToS3(JSONObject json, String s3BucketName, String s3SchemaPath, String KEYID, String SECRETKEY) throws IOException {

        // set credentials
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(KEYID, SECRETKEY);

        // initialize aws access
        AmazonS3 s3Client = AmazonS3ClientBuilder
                            .standard()
                            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                            .build();

        // convert json object to streamable bytes and store in s3
        String jsonString = json.toJSONString();
        byte[] contentBytes = jsonString.getBytes();
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        s3Client.putObject(new PutObjectRequest(s3BucketName, s3SchemaPath, inputStream, metadata));
    }

    /*                                                *
     *                                                *
     *                 HELPER CLASSES                 *
     *                                                *
     *                                                */

    // check if a string represents a date
    private static boolean isDate(String str) {

        // object for different date formats
        final String[] DATE_FORMATS = {
            "yyyy-MM-dd",
            "MM/dd/yyyy",
            "dd/MM/yyyy",
            "yyyy:MM:dd",
            "MM:dd:yyyy",
            "dd:MM:yyyy",
        };

        for (String format : DATE_FORMATS) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);

                // disable to ensure strict validation only
                sdf.setLenient(false);

                // if able to pase, then valid date
                sdf.parse(str);
                return true; 
            } catch (java.text.ParseException e) {}
        }

        // if no parse, then invalid date
        return false; 
    }

    // check if a string represents a timestamp
    private static boolean isTimestamp(String str) {

        // object for different timestamp formats
        final String[] TS_FORMATS = {
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.SSS",
        };

        for (String format : TS_FORMATS) {
        try {
            // parse timestamp using DateTimeFormatter
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            LocalDateTime.parse(str, formatter);

            // if able to pase, then valid ts
            return true;
        } catch (DateTimeParseException e) {}
    }
        // if no parse, then invalid ts
        return false;
    }

    // check if string is double
    private static boolean isDouble(String str) {
        try {
            Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // check if string is integer
    private static boolean isInteger(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
