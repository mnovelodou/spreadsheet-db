package com.novelosoftware.spreadsheetdb.controller;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.novelosoftware.spreadsheetdb.service.RecordService;

@RestController
@RequestMapping("/records")
public class RecordController {

    private final RecordService recordService;

    @Autowired
    public RecordController(RecordService recordService) {
        this.recordService = recordService;
    }

    @PostMapping("/{schemaName}")
    public ResponseEntity<Void> upsertRecord(
            @PathVariable String schemaName,
            @RequestBody String jsonData) {  // Receive JSON as raw string
        try {
            recordService.upsert(schemaName, jsonData);
            return ResponseEntity.ok().build();
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/{schemaName}/{recordKey}")
    public ResponseEntity<String> getRecord(
            @PathVariable String schemaName,
            @PathVariable String recordKey) {
        return recordService.getRecord(schemaName, recordKey)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

