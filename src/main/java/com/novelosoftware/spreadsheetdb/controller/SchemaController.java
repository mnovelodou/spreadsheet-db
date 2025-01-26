package com.novelosoftware.spreadsheetdb.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.novelosoftware.spreadsheetdb.dto.SchemaRequest;
import com.novelosoftware.spreadsheetdb.service.SchemaService;

@RestController
@RequestMapping("/schema")
public class SchemaController {

    private final SchemaService schemaService;

    public SchemaController(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    @PostMapping("/{schemaName}")
    public ResponseEntity<String> createSchema(
            @PathVariable String schemaName,
            @RequestBody SchemaRequest schemaRequest) {
        try {
            schemaService.createSchema(schemaName, schemaRequest);
            return ResponseEntity.status(HttpStatus.CREATED).body("Schema created successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Error: " + e.getMessage());
        }
    }
}
