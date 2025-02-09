package com.novelosoftware.spreadsheetdb.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.novelosoftware.spreadsheetdb.dao.SchemaDao;
import com.novelosoftware.spreadsheetdb.model.InternalSchemaModel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service class responsible for handling record operations, including upsert and retrieval.
 * Utilizes SchemaDAO to load and cache schemas and provides methods to convert JSON data to GenericRecord and vice versa.
 */
@Service
public class RecordService {

    private final SchemaDao schemaDao;
    private final Map<String, InternalSchemaModel> schemaCache = new ConcurrentHashMap<>();
    private final Map<Object, GenericRecord> recordStore = new ConcurrentHashMap<>();

    /**
     * Constructs a RecordService with the specified SchemaDAO.
     *
     * @param schemaDao the SchemaDao to be used for schema operations
     */
    @Autowired
    public RecordService(SchemaDao schemaDao) {
        this.schemaDao = schemaDao;
    }

    /**
     * Upserts a record by converting the provided JSON data to a GenericRecord and storing it.
     * If the key field is present in the record, it updates the existing record; otherwise, it creates a new one.
     *
     * @param schemaName the name of the schema
     * @param jsonData   the JSON data to be converted and stored
     * @throws IOException if an error occurs during JSON to GenericRecord conversion
     */
    public void upsert(String schemaName, String jsonData) throws IOException {
        // Retrieve schema from cache or load from DAO
        InternalSchemaModel schemaModel = getInternalSchema(schemaName);
        Schema avroSchema = schemaModel.getSchema();

        // Convert JSON to GenericRecord
        GenericRecord record = convertJsonToGenericRecord(jsonData, avroSchema);

        // Retrieve key field index from schema
        int keyIndex = schemaModel.getKeyColumnIndex();
        Schema.Field keyField = avroSchema.getFields().get(keyIndex);
        String keyFieldName = keyField.name();

        // Check if the key is already present in the record
        if (record.hasField(keyFieldName)) {
            // If key is present, use it to update the record
            Schema.Type keyType = keyField.schema().getType();
            Object key = record.get(keyFieldName);
            if (keyType != Schema.Type.LONG && keyType != Schema.Type.INT) {
                key = key.toString();
            }
            recordStore.put(key, record);
        } else {
            // If key is not present, throw an exception
            throw new IllegalArgumentException("Record must contain the 'id' field.");
        }
    }

    /**
     * Retrieves a record by its key, converting the stored GenericRecord back to a JSON object.
     *
     * @param schemaName the name of the schema
     * @param recordKey  the key of the record to retrieve
     * @return a Map representing the JSON object of the record, or null if not found
     */
    public Optional<String> getRecord(String schemaName, String stringRecordKey) {
        // Retrieve schema from cache or load from DAO
        InternalSchemaModel schemaModel = getInternalSchema(schemaName);
        Schema.Type keyType = schemaModel
                .getSchema()
                .getFields()
                .get(schemaModel.getKeyColumnIndex())
                .schema()
                .getType();

        Object recordKey = stringRecordKey;

        if (keyType == Schema.Type.LONG) {
            recordKey = Long.parseLong(stringRecordKey);
        } else if (keyType == Schema.Type.INT) {
            recordKey = Integer.parseInt(stringRecordKey);
        }  // else key is a string

        Optional<GenericRecord> maybeRecord = Optional.ofNullable(recordStore.get(recordKey));
        return maybeRecord.map(record -> convertGenericRecordToJson(record, schemaModel.getSchema()));
    }

    private InternalSchemaModel getInternalSchema(String schemaName) {
        return schemaCache.computeIfAbsent(schemaName, key -> {
            try {
                return schemaDao.loadSchema(key);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load schema: " + key, e);
            }
        });
    }

    /**
     * Decodes json into a GenericRecord
     * 
     * @param jsonData json string with the record
     * @param avroSchema schema for decoding the record
     * @return the GenericRecord which is an avro-schema
     * @throws IOException
     */
    private GenericRecord convertJsonToGenericRecord(String jsonData, Schema avroSchema) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(avroSchema, jsonData);
        return datumReader.read(null, jsonDecoder);
    }

    private String convertGenericRecordToJson(GenericRecord record, Schema avroSchema) {
        try {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(avroSchema, out);
            datumWriter.write(record, jsonEncoder);
            jsonEncoder.flush();
            return out.toString("UTF-8");
        } catch (IOException ex) {
            throw new RuntimeException("Error while converting generic record to json", ex);
        }
    }
}
