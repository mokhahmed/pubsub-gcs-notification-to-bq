package com.google.gdc.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.JsonObject;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.util.StreamUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper object to parse a JSON on GCS. Usage is to provide A GCS URL and it will return a
 * JSONObject of the file
 */
public class SchemaParser {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaParser.class);

    /**
     * Parses a JSON file and Returns a JSONObject containing the necessary schema information.
     *
     * @param pathToJSON the JSON file location, so we can download and parse it
     * @return the parsed JSONObject
     */
    public static JSONObject parseSchemaFile(String pathToJSON) throws Exception {
        try {
            ReadableByteChannel readableByteChannel = FileSystems.open(
                    FileSystems.matchNewResource(pathToJSON, false));

            String json = new String(StreamUtils.getBytesWithoutClosing(
                    Channels.newInputStream(readableByteChannel)));

            return new JSONObject(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static JSONObject parseSchemaString(String schemaString) throws Exception {
        return new JSONObject(schemaString);
    }

    /**
     * Converts a JSON string to a {@link TableRow} object. If the data fails to convert, a {@link
     * RuntimeException} will be thrown.
     *
     * @param json The JSON string to parse.
     * @return The parsed {@link TableRow} object.
     */
    public static TableRow convertJsonToTableRow(String json) {
        TableRow row;
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }
        return row;
    }


    public static  TableSchema convertJsonToTableSchema(String pathToJSON) throws Exception {
        JSONObject schema =  parseSchemaFile(pathToJSON);
        List<TableFieldSchema> fields = new ArrayList<>();
        for (String k : schema.keySet())
            fields.add(new TableFieldSchema().setName(k).setType(schema.getString(k)));
        return new TableSchema().setFields(fields);
    }

}
