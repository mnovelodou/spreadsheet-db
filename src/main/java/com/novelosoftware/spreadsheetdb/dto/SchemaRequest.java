package com.novelosoftware.spreadsheetdb.dto;

import org.apache.avro.Schema;
import lombok.Getter;

@Getter
public class SchemaRequest {
    private Schema avroSchema; // Avro schema as a JSON string
    private String keyColumn; // Name of the key column
}
