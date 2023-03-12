package org.alfasoftware.morf.avro;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.directory.DirectoryDataSetConsumer;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.DataValueLookup;
import org.alfasoftware.morf.metadata.Table;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class AvroDataSetConsumer extends DirectoryDataSetConsumer implements DataSetConsumer {

    private static final Gson GSON_FOR_TABLE_DATA = new GsonBuilder().setPrettyPrinting().registerTypeAdapter(Table.class, new JsonTableSerializer()).create();
    private static final Gson GSON_FOR_COLUMN_DATA = new GsonBuilder().setPrettyPrinting().registerTypeAdapter(Table.class, new JsonColumnSetSerializer()).create();
    private final Map<DataType, BiFunction<Record, String, Object>> recordMappingFunctions = new EnumMap<>(DataType.class);

    public AvroDataSetConsumer(Path path) {
        super("avro", path);
        recordMappingFunctions.put(DataType.DATE, DataValueLookup::getDate);
        recordMappingFunctions.put(DataType.BIG_INTEGER, DataValueLookup::getLong);
        recordMappingFunctions.put(DataType.BLOB, DataValueLookup::getByteArray);
        recordMappingFunctions.put(DataType.BOOLEAN, DataValueLookup::getBoolean);
        recordMappingFunctions.put(DataType.CLOB, DataValueLookup::getByteArray);
        recordMappingFunctions.put(DataType.DECIMAL, DataValueLookup::getBigDecimal);
        recordMappingFunctions.put(DataType.INTEGER, DataValueLookup::getInteger);
        recordMappingFunctions.put(DataType.STRING, DataValueLookup::getString);
    }

    @Override
    public void open() {
        super.directoryOutputStreamProvider.open();
    }

    @Override
    public void close(CloseState closeState) {
        super.directoryOutputStreamProvider.close();
    }

    @Override
    public void table(Table table, Iterable<Record> records) {
        String gsonSchema = GSON_FOR_COLUMN_DATA.toJson(table, Table.class);
        Map<String, DataType> columnNamesAndTypes = table.columns().stream().collect(Collectors.toMap(Column::getName, Column::getType));
        Schema avroSchema = new Schema.Parser().parse(gsonSchema);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(avroSchema, super.directoryOutputStreamProvider.openOutputStreamForTable(table.getName()));
            for (Record r : records) {
                GenericRecord row = new GenericData.Record(avroSchema);
                columnNamesAndTypes.entrySet().forEach(entry -> row.put(entry.getKey(), recordMappingFunctions.get(entry.getValue()).apply(r, entry.getKey())));
                dataFileWriter.append(row);
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception occurred upon trying to write avro data file for table [" + table.getName() + "]", e);
        }
    }
}
