package org.alfasoftware.morf.avro;

import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Schema;

public class AvroDataSetProducer implements DataSetProducer {
    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public Schema getSchema() {
        return null;
    }

    @Override
    public Iterable<Record> records(String tableName) {
        return null;
    }

    @Override
    public boolean isTableEmpty(String tableName) {
        return false;
    }
}
