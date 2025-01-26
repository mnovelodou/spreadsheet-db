package com.novelosoftware.spreadsheetdb.serialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.avro.Schema;

import java.io.IOException;

/**
 * AvroSchemaDeserializer Adds logic for parse the avro schemas from the JSON payloads
 */
public class AvroSchemaDeserializer extends JsonDeserializer<Schema> {

    @Override
    public Schema deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String schemaJson = p.readValueAsTree().toString(); // Read the schema as a string
        return new Schema.Parser().parse(schemaJson); // Parse it into an Avro schema
    }
}
