package org.healthetl.filters;

import org.healthetl.data.S3SchemaWriter;
import org.healthetl.utils.DataTypeInfererUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;


public class SchemaDefinitionFilter {
    private final DataTypeInfererUtil dataTypeInferrer;
    private final S3SchemaWriter s3DataWriter;

    public SchemaDefinitionFilter(DataTypeInfererUtil dataTypeInferrer, S3SchemaWriter s3DataWriter) {
        this.dataTypeInferrer = dataTypeInferrer;
        this.s3DataWriter = s3DataWriter;
    }

    // Method to infer data types and write the result to an S3 bucket
    public void schemaLog(JSONArray json) throws IOException, InterruptedException {
        JSONObject schemaDefinition = inferSchemaDefinition(json);
        writeSchemaToS3(schemaDefinition);
    }

    private JSONObject inferSchemaDefinition(JSONArray jsonInput) {
        return dataTypeInferrer.inferDataTypes(jsonInput);
    }
    private void writeSchemaToS3(JSONObject schemaDefinition) {
        s3DataWriter.writeJsonToS3(schemaDefinition);
    }
}