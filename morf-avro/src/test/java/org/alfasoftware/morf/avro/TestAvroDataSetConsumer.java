package org.alfasoftware.morf.avro;

import com.google.common.collect.Lists;
import com.google.common.jimfs.Jimfs;
import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.junit.Assert.assertEquals;

public class TestAvroDataSetConsumer {

    private static final String SUB_DIR_FOR_TEST_CLASS = TestAvroDataSetConsumer.class.getName();
    private FileSystem fileSystem;
    private Path root;


    @Before
    public void setup() throws IOException {
        fileSystem = Jimfs.newFileSystem();
        root = fileSystem.getPath("/" + SUB_DIR_FOR_TEST_CLASS);
        Files.createDirectory(root);
    }

    @Test
    public void testOutputOneFile() throws IOException {
        // Given
        Path pathForThisTest = root.resolve("testOutputOneFile");
        Files.createDirectory(pathForThisTest);
        AvroDataSetConsumer sut = new AvroDataSetConsumer(pathForThisTest);
        LinkedHashMap<Long, String> inputs = new LinkedHashMap<>();
        inputs.put(1L, "Bran");
        inputs.put(2L, "Brannan");
        DataSetProducer producer = new StubDataSetProducer(inputs);

        // When
        new DataSetConnector(producer, sut).connect();

        // Then - read content from in memory file system.
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        Files.createDirectories(new File(System.getProperty("user.dir")).toPath().resolve("target/testOutputOneFile"));
        // copy out of the in memory file system as avro doesn't support the java.nio paths api :(
        Files.copy(pathForThisTest.resolve("avrotable.avro"), Files.newOutputStream(new File(System.getProperty("user.dir")).toPath().resolve("target/testOutputOneFile/avrotable.avro")));
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(new File(System.getProperty("user.dir")).toPath().resolve("target/testOutputOneFile/avrotable.avro").toString()), datumReader);
        List<GenericRecord> rows = new ArrayList<>();
        while (dataFileReader.hasNext()) {
            rows.add(dataFileReader.next());
        }
        assertEquals("Column1 for row 1 should be correct", 1L, rows.get(0).get("Column1"));
        assertEquals("Column2 for row 1 should be correct", inputs.get(1L), rows.get(0).get("Column2").toString());
        assertEquals("Column1 for row 2 should be correct", 2L, rows.get(1).get("Column1"));
        assertEquals("Column2 for row 2 should be correct", inputs.get(2L), rows.get(1).get("Column2").toString());
    }


    @Test
    public void testOutputOneFileAsArchive() throws IOException {
        // Given
        Path pathForThisTest = root.resolve("testOutputOneFileAsArchive");
        AvroDataSetConsumer sut = new AvroDataSetConsumer(pathForThisTest);
        LinkedHashMap<Long, String> inputs = new LinkedHashMap<>();
        inputs.put(1L, "Bran");
        inputs.put(2L, "Brannan");
        DataSetProducer producer = new StubDataSetProducer(inputs);

        // When
        new DataSetConnector(producer, sut).connect();

        // Then - read content from in memory file system.
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        Files.createDirectories(new File(System.getProperty("user.dir")).toPath().resolve("target/testOutputOneFileAsArchive"));
        // copy out of the in memory file system as avro doesn't support the java.nio paths api :(
        Path outputPath = new File(System.getProperty("user.dir")).toPath().resolve("target/testOutputOneFileAsArchive");
        Files.copy(pathForThisTest, Files.newOutputStream(outputPath.resolve("output.zip")));

        ZipFile zipFile = new ZipFile(outputPath.resolve("output.zip").toFile());
        ZipEntry zipEntry = zipFile.getEntry("avrotable.avro");
        zipFile.getInputStream(zipEntry).transferTo(Files.newOutputStream(outputPath.resolve("extractedFromZip.avro"), CREATE_NEW));
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(new File(System.getProperty("user.dir")).toPath().resolve("target/testOutputOneFileAsArchive/extractedFromZip.avro").toString()), datumReader);
        List<GenericRecord> rows = new ArrayList<>();
        while (dataFileReader.hasNext()) {
            rows.add(dataFileReader.next());
        }
        assertEquals("Column1 for row 1 should be correct", 1L, rows.get(0).get("Column1"));
        assertEquals("Column2 for row 1 should be correct", inputs.get(1L), rows.get(0).get("Column2").toString());
        assertEquals("Column1 for row 2 should be correct", 2L, rows.get(1).get("Column1"));
        assertEquals("Column2 for row 2 should be correct", inputs.get(2L), rows.get(1).get("Column2").toString());
    }

    private static class StubDataSetProducer implements DataSetProducer {

        private final Map<Long, String> inputRecords;

        StubDataSetProducer(Map<Long, String> inputRecords) {
            this.inputRecords = inputRecords;
        }

        @Override
        public void open() {
           // no-op
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public Schema getSchema() {
            return new Schema() {
                @Override
                public boolean isEmptyDatabase() {
                    return false;
                }

                @Override
                public boolean tableExists(String name) {
                    return name.equalsIgnoreCase("avrotable");
                }

                @Override
                public Table getTable(String name) {
                    if (!name.equalsIgnoreCase("avrotable")) {
                        throw new IllegalStateException("Asked for a table which doesn't exist: [" + name + "]");
                    }
                    return new AvroTable();
                }

                @Override
                public Collection<String> tableNames() {
                    return Lists.newArrayList("avrotable");
                }

                @Override
                public Collection<Table> tables() {
                    return Lists.newArrayList(new AvroTable());
                }

                @Override
                public boolean viewExists(String name) {
                    return false;
                }

                @Override
                public View getView(String name) {
                    throw new IllegalStateException("Asked for a view which doesn't exist: [" + name + "]");
                }

                @Override
                public Collection<String> viewNames() {
                    return new ArrayList<>();
                }

                @Override
                public Collection<View> views() {
                    return new ArrayList<>();
                }
            };
        }

        @Override
        public Iterable<Record> records(String tableName) {
            if (!tableName.equalsIgnoreCase("avrotable")) {
                throw new RuntimeException("Asked for record of table which doesn't exist: [" + tableName + "]");
            }
            return inputRecords.entrySet().stream()
                    .map(entry -> DataSetUtils.record().setLong("Column1", entry.getKey())
                                                       .setString("Column2", entry.getValue()))
                    .collect(Collectors.toList());
        }

        @Override
        public boolean isTableEmpty(String tableName) {
            if (!tableName.equalsIgnoreCase("avrotable")) {
                throw new RuntimeException("Asked for record of table which doesn't exist: [" + tableName + "]");
            }
            return false;
        }
    }

    private static class StubColumn implements Column {

        private final String name;
        private final boolean primaryKey;
        private final String defaultValue;
        private final boolean autoNumbered;
        private final int autoNumberStart;
        private final DataType dataType;
        private final int width;
        private final int scale;
        private final boolean nullable;


        StubColumn(String name, boolean primaryKey, String defaultValue, boolean autoNumbered, int autoNumberStart, DataType dataType, int width, int scale, boolean nullable) {

            this.name = name;
            this.primaryKey = primaryKey;
            this.defaultValue = defaultValue;
            this.autoNumbered = autoNumbered;
            this.autoNumberStart = autoNumberStart;
            this.dataType = dataType;
            this.width = width;
            this.scale = scale;
            this.nullable = nullable;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isPrimaryKey() {
            return primaryKey;
        }

        @Override
        public String getDefaultValue() {
            return defaultValue;
        }

        @Override
        public boolean isAutoNumbered() {
            return autoNumbered;
        }

        @Override
        public int getAutoNumberStart() {
            return autoNumberStart;
        }

        @Override
        public DataType getType() {
            return dataType;
        }

        @Override
        public int getWidth() {
            return width;
        }

        @Override
        public int getScale() {
            return scale;
        }

        @Override
        public boolean isNullable() {
            return nullable;
        }
    }

    private static class AvroTable implements Table {
        @Override
        public String getName() {
            return "avrotable";
        }

        @Override
        public List<Column> columns() {
            return Lists.newArrayList(
                    new StubColumn("Column1", true, "1", true, 1, DataType.BIG_INTEGER, 12, 0, false),
                    new StubColumn("Column2", false, "Hello", false, 1, DataType.STRING, 20, 0, false)
            );
        }

        @Override
        public List<Index> indexes() {
            return new ArrayList<>();
        }

        @Override
        public boolean isTemporary() {
            return false;
        }
    }
}
