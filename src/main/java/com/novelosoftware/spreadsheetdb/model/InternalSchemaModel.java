package com.novelosoftware.spreadsheetdb.model;

import org.apache.avro.Schema;

/**
 * Represents an internally stored schema, including:
 * - The schema name.
 * - The Avro schema itself.
 * - The key column index within the schema.
 */
public class InternalSchemaModel {

    private final String schemaName;
    private final Schema schema;
    private final int keyColumnIndex;

    /**
     * Constructs an InternalSchemaModel.
     *
     * @param schemaName     the name of the schema
     * @param schema         the Avro schema
     * @param keyColumnIndex the index of the key column within the schema
     */
    public InternalSchemaModel(String schemaName, Schema schema, int keyColumnIndex) {
        this.schemaName = schemaName;
        this.schema = schema;
        this.keyColumnIndex = keyColumnIndex;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getKeyColumnIndex() {
        return keyColumnIndex;
    }

    @Override
    public String toString() {
        return "InternalSchemaModel{" +
                "schemaName='" + schemaName + '\'' +
                ", keyColumnIndex=" + keyColumnIndex +
                ", schema=" + schema.toString() +
                '}';
    }
}
