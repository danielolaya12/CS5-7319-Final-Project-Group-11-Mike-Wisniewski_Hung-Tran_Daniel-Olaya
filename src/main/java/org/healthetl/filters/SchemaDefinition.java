package org.healthetl.filters;

import org.json.simple.JSONArray;
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



public class SchemaDefinition {

    // Method to infer data types and write the result to an S3 bucket
    public static void schemaLog(String jsonInput, String s3BucketName, String s3SchemaPath) throws ParseException, IOException {

        // create schema definition
        JSONObject schemaDefinition = inferDataTypes(jsonInput);

        // output to s3
        writeJsonToS3(schemaDefinition, s3BucketName, s3SchemaPath);
    }

    private static JSONObject inferDataTypes(String jsonInput) {

        try {
            // create json objects
            JSONObject result = new JSONObject();
            JSONParser parser = new JSONParser();
            JSONArray jsonArray = (JSONArray) parser.parse(jsonInput);

            // only need first entry from json -- ASSUMPTION
            JSONObject firstObject = (JSONObject) jsonArray.get(0);

            // iterate and infer data types
            for (Object entry : firstObject.entrySet()) {
                Map.Entry<String, Object> keyValue = (Map.Entry<String, Object>) entry;
                String key = keyValue.getKey();
                Object value = keyValue.getValue();
                Class<?> valueType = inferValueType(value);
                result.put(key, valueType != null ? valueType.getSimpleName() : "Unknown");
            }
            return result;

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
        
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

    private static void writeJsonToS3(JSONObject json, String s3BucketName, String s3SchemaPath) throws IOException {

        // AWS creds 
        // TODO: change to windows path
        String kid = "";
        String sak = "";

        // set credentials
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(kid, sak);

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

    public static void main(String[] args) {
        // String jsonInput = args[0];
        String jsonInput = "[{\"code\":\"15777000\",\"stop\":\"2012-12-08\",\"patient\":\"8b9de29c-34c9-405f-840c-71b5210cf9e1\",\"start\":\"2003-04-03\",\"description\":\"Prediabetes\",\"encounter\":\"c8fa6e8f-5557-4316-90cc-f27d1d76de7a\"},\n" +
        "{\"code\":\"10509002\",\"stop\":\"2012-12-08\",\"patient\":\"8b9de29c-34c9-405f-840c-71b5210cf9e1\",\"start\":\"2012-11-27\",\"description\":\"Acute bronchitis (disorder)\",\"encounter\":\"e277ecc6-5cbf-4b8f-958c-cdbd171269c3\"}]";
        String s3BucketName = "cs7319";
        String s3SchemaPath = "schema_log/schema_definition.json";
        try {
            schemaLog(jsonInput, s3BucketName, s3SchemaPath);
        } catch (ParseException | IOException e) {
            e.printStackTrace();
        }
    }
}