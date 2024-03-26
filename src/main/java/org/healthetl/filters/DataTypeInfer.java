package org.healthetl.filters;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.HashMap;
import java.util.Map;

public class DataTypeInfer {

    // TODO: Mike and Hung
    // schemaLog -> writing to S3

    // parquetWriter -> writing to s3

    // TODO: Scheduler - Daniel - kicks off the Main

    // TODO: metaDataLogger - Daniel - logs when scheduler started and ended

    public static JSONObject inferDataTypes(String jsonInput) throws ParseException {
        
        // initialize json object to store output result
        JSONObject result = new JSONObject();

        // initialize parser
        JSONParser parser = new JSONParser();

        // initialize array
        JSONArray jsonArray = (JSONArray) parser.parse(jsonInput);

        // extract first json entry
        JSONObject firstObject = (JSONObject) jsonArray.get(0);

        // for each column in the first entry of the json
        for (Object entry : firstObject.entrySet()) {
            Map.Entry<String, Object> keyValue = (Map.Entry<String, Object>) entry;
            String key = keyValue.getKey();
            Object value = keyValue.getValue();
            Class<?> valueType = inferValueType(value);
            result.put(key, valueType != null ? valueType.getSimpleName() : "Unknown");
        }

        return result;
    }

    private static Class<?> inferValueType(Object value) {
        if (value instanceof String) {
            return String.class;
        } else if (value instanceof Number) {
            Number number = (Number) value;
            if (number instanceof Integer || number instanceof Long) {
                return Long.class;
            } else if (number instanceof Double || number instanceof Float) {
                return Double.class;
            }
        } else if (value instanceof Boolean) {
            return Boolean.class;
        }
        return null;
    }

    public static void main(String[] args) {
        String jsonInput = "[{\"code\":\"15777000\",\"stop\":\"2012-12-08\",\"patient\":\"8b9de29c-34c9-405f-840c-71b5210cf9e1\",\"start\":\"2003-04-03\",\"description\":\"Prediabetes\",\"encounter\":\"c8fa6e8f-5557-4316-90cc-f27d1d76de7a\"},\n" +
                "{\"code\":\"10509002\",\"stop\":\"2012-12-08\",\"patient\":\"8b9de29c-34c9-405f-840c-71b5210cf9e1\",\"start\":\"2012-11-27\",\"description\":\"Acute bronchitis (disorder)\",\"encounter\":\"e277ecc6-5cbf-4b8f-958c-cdbd171269c3\"}]";

        try {
            JSONObject inferredTypes = inferDataTypes(jsonInput);
            System.out.println(inferredTypes);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}