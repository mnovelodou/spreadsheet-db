package com.novelosoftware.spreadsheetdb.dao;

import org.apache.avro.Schema;
import org.apache.avro.SchemaFormatter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.novelosoftware.spreadsheetdb.model.InternalSchemaModel;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Data Access Object (DAO) for storing and retrieving Avro schemas along with key column metadata.
 * <p>
 * The key column position is stored as an integer at the beginning of the file,
 * followed by the Avro schema in binary format.
 * </p>
 */
@Component
public class SchemaDao {

    private final String storagePath;
    private static final String FILE_EXTENSION = ".avsc.bin"; // Binary Avro schema file extension
    private static final int KEY_COLUMN_NOT_FOUND = -1; // Key column not found in schema

    /**
     * Constructs a SchemaDAO with the given storage path.
     *
     * @param storagePath the directory where schemas will be stored
     */
    public SchemaDao(@Value("${schema.storage.path}") String storagePath) {
        this.storagePath = storagePath;
    }

    /**
     * Saves an Avro schema along with the key column position.
     *
     * @param schemaName the name of the schema (used as the filename)
     * @param keyColumn  the key column name
     * @param schema     the Avro schema to store
     * @throws IOException if an error occurs while writing the file
     */
    public void saveSchema(String schemaName, String keyColumn, Schema schema) throws IOException {
        File directory = new File(storagePath);
        if (!directory.exists()) {
            directory.mkdirs(); // Ensure the directory exists
        }

        File file = new File(directory, schemaName + FILE_EXTENSION);

        // Find key column position
        int keyColumnIndex = findKeyColumnIndex(schema, keyColumn);
        if (keyColumnIndex == KEY_COLUMN_NOT_FOUND) {
            throw new IllegalArgumentException("Key column '" + keyColumn + "' not found in schema.");
        }

        // Write key column index + Avro schema in binary format
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
            out.writeInt(keyColumnIndex); // Store key column position
            byte[] schemaBytes = compressSchemaToJson(schema);
            out.write(schemaBytes, 0, schemaBytes.length); // Write Avro schema
        }
    }

    /**
     * Loads an Avro schema along with the key column position.
     *
     * @param schemaName the name of the schema to load
     * @return an {@link InternalSchemaModel} containing the schema, schema name, and key column position
     * @throws IOException if an error occurs while reading the file
     */
    public InternalSchemaModel loadSchema(String schemaName) throws IOException {
        File file = new File(storagePath, schemaName + FILE_EXTENSION);
        if (!file.exists()) {
            throw new FileNotFoundException("Schema file not found: " + file.getAbsolutePath());
        }

        try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
            int keyColumnIndex = in.readInt(); // Read key column position

            // Read remaining bytes (which contain the Avro schema)
            byte[] schemaBytes = in.readAllBytes();
            Schema schema = decompressSchemaFromJson(schemaBytes);
            return new InternalSchemaModel(schemaName, schema, keyColumnIndex);
        }
    } 

    /**
     * Compresses an Avro Schema by converting it to a compact JSON representation
     * and then applying GZIP compression.
     *
     * @param schema The Avro Schema to compress.
     * @return A byte array containing the compressed JSON schema.
     * @throws IOException If an error occurs during compression.
     */
    public static byte[] compressSchemaToJson(Schema schema) throws IOException {
        // Convert Schema to Compact JSON
        String json = schema.toString(); // false -> Compact format

        // Compress JSON using GZIP
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipOut = new GZIPOutputStream(byteStream)) {
            gzipOut.write(json.getBytes());
            gzipOut.finish(); // Ensure all data is written
            return byteStream.toByteArray();
        }
    }

    /**
     * Decompresses a GZIP-compressed JSON Avro Schema and converts it back to an Avro Schema object.
     *
     * @param compressedSchema The compressed byte array containing the JSON schema.
     * @return The reconstructed Avro Schema.
     * @throws IOException If an error occurs during decompression or parsing.
     */
    public static Schema decompressSchemaFromJson(byte[] compressedSchema) throws IOException {
        // Decompress GZIP data
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(compressedSchema);
             GZIPInputStream gzipIn = new GZIPInputStream(byteStream);
             InputStreamReader reader = new InputStreamReader(gzipIn);
             BufferedReader bufferedReader = new BufferedReader(reader)) {

            // Read the decompressed JSON string
            StringBuilder jsonBuilder = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                jsonBuilder.append(line);
            }

            // Convert JSON back to Schema
            return new Schema.Parser().parse(jsonBuilder.toString());
        }
    }

    /**
     * Finds the index of the key column in the Avro schema.
     *
     * @param schema    the Avro schema
     * @param keyColumn the name of the key column
     * @return the index of the key column in the schema, or -1 if not found
     */
    private int findKeyColumnIndex(Schema schema, String keyColumn) {
        if (schema.getType() == Schema.Type.RECORD) {
            int index = 0;
            for (Schema.Field field : schema.getFields()) {
                if (field.name().equals(keyColumn)) {
                    return index;
                }
                index++;
            }
        }
        return KEY_COLUMN_NOT_FOUND; // Key column not found
    }
}
