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

public class JsonTableSerializer implements JsonSerializer<Table> {

    private final Map<DataType, String> DATA_TYPE_TO_AVRO_TYPE_MAP;

    JsonTableSerializer() {
        DATA_TYPE_TO_AVRO_TYPE_MAP = new EnumMap<DataType, String>(DataType.class);
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.DATE, "int");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.BIG_INTEGER, "long");
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.BLOB, "string");
//        recordMappingFunctions.put(DataType.BOOLEAN, (record, columnName) -> record.getBoolean(columnName));
//        recordMappingFunctions.put(DataType.CLOB, (record, columnName) -> record.getByteArray(columnName));
//        recordMappingFunctions.put(DataType.DECIMAL, (record, columnName) -> record.getBigDecimal(columnName));
//        recordMappingFunctions.put(DataType.INTEGER, (record, columnName) -> record.getInteger(columnName));
        DATA_TYPE_TO_AVRO_TYPE_MAP.put(DataType.STRING, "string");
    }

    @Override
    public JsonElement serialize(Table table, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonElement jsonElement = new JsonObject();
        jsonElement.getAsJsonObject().addProperty("namespace", "org.alfasoftware.morf");
        jsonElement.getAsJsonObject().addProperty("name", "table");
        jsonElement.getAsJsonObject().addProperty("type", "record");
        JsonElement fields = new JsonArray();
        JsonElement isTemporary = new JsonObject();
        isTemporary.getAsJsonObject().addProperty("name", "isTemporary");
        isTemporary.getAsJsonObject().addProperty("type", "boolean");
        fields.getAsJsonArray().add(isTemporary);
        JsonElement columnType = new JsonObject();
        columnType.getAsJsonObject().addProperty("name", "column");
        columnType.getAsJsonObject().addProperty("type", "record");
        columnType.getAsJsonObject().add("fields", getColumnsAsFieldsFor(table.columns()));
        JsonElement columns = new JsonObject();
        columns.getAsJsonObject().addProperty("name", "columns");
        columns.getAsJsonObject().add("type", columnType);
        fields.getAsJsonArray().add(columns);
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
