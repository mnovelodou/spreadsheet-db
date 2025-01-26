package com.novelosoftware.spreadsheetdb.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.novelosoftware.spreadsheetdb.dto.CreateSchemaRequest;
import com.novelosoftware.spreadsheetdb.service.SchemaService;

/**
 * SchemaController defines the REST API for creating a table schema. 
 */
@RestController
@RequestMapping("/schema")
public class SchemaController {

    private final SchemaService schemaService;

    public SchemaController(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    /**
     * createSchema creates a table using an avro schema
     * @param schemaName name of the table/schema
     * @param schemaRequest request body for creating the schema
     * @return ResponseEntity with the result of this operation.
     */
    @PostMapping("/{schemaName}")
    public ResponseEntity<String> createSchema(
            @PathVariable String schemaName,
            @RequestBody CreateSchemaRequest schemaRequest) {
        try {
            schemaService.createSchema(schemaName, schemaRequest);
            return ResponseEntity.status(HttpStatus.CREATED).body("Schema created successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Error: " + e.getMessage());
        }
    }
}
