package com.google.gdc.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.json.JSONObject;

import java.util.List;

public class Utilities {


    public  static String extractPath(String notification, String gscPrefix) {
        try {
            JSONObject event = new SchemaParser().parseSchemaString(notification).getJSONObject("event");
            return gscPrefix + event.get("bucket").toString() + "/" + event.get("name").toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static TableRow convertStringToBqRow(String record, TableSchema tableSchema, String delimiter) {
        List<TableFieldSchema> fieldsSchema = tableSchema.getFields();

        String[] fields = record.split(delimiter);

        TableRow row = new TableRow();

        for (int i = 0; i < fieldsSchema.size(); i++)
            row.set(fieldsSchema.get(i).getName(), fields[i]);

        return row;
    }

}
