package com.novelosoftware.spreadsheetdb.service;

import org.springframework.stereotype.Service;
import com.novelosoftware.spreadsheetdb.dto.CreateSchemaRequest;

/**
 * SchemaService has the business logic which validates an schema creation and
 * manages storing the schema and caching the schema for record retrieval.
 */
@Service
public class SchemaService {

    /**
     * Creates a new table using the requested avro schema.
     */
    public void createSchema(String schemaName, CreateSchemaRequest schemaRequest) {
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
