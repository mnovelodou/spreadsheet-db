package com.novelosoftware.spreadsheetdb.dto;

import org.apache.avro.Schema;
import lombok.Getter;

/**
 * CreateSchemaRequest is used as the body payload for POST on /schema/
 */
@Getter
public class CreateSchemaRequest {
    
    /**
     * avroSchema is the actual avro schema that defines the table fields.
     */
    private Schema avroSchema;

    /**
     * keyColumn indicates which field of the avro schema corresponds to the table key.
     */
    private String keyColumn;
}
