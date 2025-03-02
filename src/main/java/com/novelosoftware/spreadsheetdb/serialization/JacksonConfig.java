package com.novelosoftware.spreadsheetdb.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.avro.Schema;

/**
 * Jackson config adds custom serialization logic like avro schema parsing.
 */
@Configuration
public class JacksonConfig {

    /**
     * objectMapper, implements the objectMapper configuration.
     */
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        // Register the custom deserializer for Avro Schema
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Schema.class, new AvroSchemaDeserializer());

        objectMapper.registerModule(module);
        return objectMapper;
    }
}
