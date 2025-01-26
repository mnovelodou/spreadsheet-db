package com.novelosoftware.spreadsheetdb.service;

import org.springframework.stereotype.Service;
import com.novelosoftware.spreadsheetdb.dto.SchemaRequest;

@Service
public class SchemaService {

    public void createSchema(String schemaName, SchemaRequest schemaRequest) {
        // Validate schemaName and schemaRequest
        if (schemaName == null || schemaName.isEmpty()) {
            throw new IllegalArgumentException("Schema name cannot be null or empty");
        }

        if (schemaRequest.getAvroSchema() == null) {
            throw new IllegalArgumentException("Avro schema cannot be null or empty");
        }

        if (schemaRequest.getKeyColumn() == null || schemaRequest.getKeyColumn().isEmpty()) {
            throw new IllegalArgumentException("Key column cannot be null or empty");
        }

        // Process the Avro schema and key column
        // Example: Parse the Avro schema, create a table, or store schema metadata
        System.out.println("Creating schema: " + schemaName);
        System.out.println("Avro Schema: " + schemaRequest.getAvroSchema());
        System.out.println("Key Column: " + schemaRequest.getKeyColumn());

        // Add logic to store the schema or interact with the database
    }
}
