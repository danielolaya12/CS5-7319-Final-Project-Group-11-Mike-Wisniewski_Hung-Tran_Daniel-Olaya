package org.healthetl.filters;

import lombok.extern.log4j.Log4j2;
import org.healthetl.data.S3SchemaWriter;
import org.healthetl.utils.DataTypeInfererUtil;
import org.json.simple.JSONObject;

@Log4j2
public class SchemaDefinitionFilter extends Filter{
    private final DataTypeInfererUtil dataTypeInferrer;
    private final S3SchemaWriter s3DataWriter;

    public SchemaDefinitionFilter(DataTypeInfererUtil dataTypeInferrer, S3SchemaWriter s3DataWriter) {
        this.dataTypeInferrer = dataTypeInferrer;
        this.s3DataWriter = s3DataWriter;
    }
    @Override
    public void run (){
        try {
            schemaLog();
        } catch (Exception e){
            log.error(e.getMessage());
        }

    }

    // Method to infer data types and write the result to an S3 bucket
    public void schemaLog() throws InterruptedException {
        JSONObject json;
        if ((json = input.read()) != null) {
            // create schema definition
            JSONObject schemaDefinition = inferSchemaDefinition(json);
            // output to s3
            writeSchemaToS3(schemaDefinition);
        }
    }

    private JSONObject inferSchemaDefinition(JSONObject jsonInput) {
        return dataTypeInferrer.inferDataTypes(jsonInput);
    }
    private void writeSchemaToS3(JSONObject schemaDefinition) {
        s3DataWriter.writeJsonToS3(schemaDefinition);
    }
}