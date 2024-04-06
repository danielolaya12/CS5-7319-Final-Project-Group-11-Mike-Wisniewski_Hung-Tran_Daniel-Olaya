package org.healthetl.utils;

import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONObject;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Map;

@Log4j2
public class DataTypeInfererUtil {
    public JSONObject inferDataTypes(JSONObject jsonInput) {
        // create json objects
        JSONObject result = new JSONObject();

        // only need first entry from json -- ASSUMPTION
        JSONObject firstObject = jsonInput;

        // iterate and infer data types
        for (Object entry : firstObject.entrySet()) {
            Map.Entry<String, Object> keyValue = (Map.Entry<String, Object>) entry;
            String key = keyValue.getKey();
            Object value = keyValue.getValue();
            Class<?> valueType = inferValueType(value);
            result.put(key, valueType != null ? valueType.getSimpleName() : "Unknown");
        }
        return result;

    }
    private Class<?> inferValueType(Object value) {

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
    private boolean isDate(String str) {

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
            } catch (java.text.ParseException e) {
                log.error(e.getMessage());
            }
        }

        // if no parse, then invalid date
        return false;
    }

    // check if a string represents a timestamp
    private boolean isTimestamp(String str) {

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
            } catch (DateTimeParseException e) {
                log.error(e.getMessage());
            }
        }
        // if no parse, then invalid ts
        return false;
    }

    // check if string is double
    private boolean isDouble(String str) {
        try {
            Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // check if string is integer
    private boolean isInteger(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
