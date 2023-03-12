package org.alfasoftware.morf.avro;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;

import java.lang.reflect.Type;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class JsonColumnSetSerializer implements JsonSerializer<Table> {

    private final Map<DataType, String> DATA_TYPE_TO_AVRO_TYPE_MAP;

    JsonColumnSetSerializer() {
        DATA_TYPE_TO_AVRO_TYPE_MAP = new EnumMap<DataType, String>(DataType.class);
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.DATE, "int");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.BIG_INTEGER, "long");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.BLOB, "string");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.BOOLEAN, "boolean");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.CLOB, "string");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.DECIMAL, "double");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.INTEGER, "int");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.STRING, "string");
    }

    @Override
    public JsonElement serialize(Table table, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonElement jsonElement = new JsonObject();
        jsonElement.getAsJsonObject().addProperty("namespace", "org.alfasoftware.morf");
        jsonElement.getAsJsonObject().addProperty("name", table.getName() + "_columns");
        jsonElement.getAsJsonObject().addProperty("type", "record");
        JsonElement fields = getColumnsAsFieldsFor(table.columns());
        jsonElement.getAsJsonObject().add("fields", fields);
        return jsonElement;
    }

    private JsonArray getColumnsAsFieldsFor(List<Column> columns) {
        JsonArray array = new JsonArray();
        for (Column c : columns) {
            JsonObject object = new JsonObject();
            object.getAsJsonObject().addProperty("name", c.getName());
            object.getAsJsonObject().addProperty("type", DATA_TYPE_TO_AVRO_TYPE_MAP.get(c.getType()));
            array.getAsJsonArray().add(object);
        }
        return array;
    }
}
