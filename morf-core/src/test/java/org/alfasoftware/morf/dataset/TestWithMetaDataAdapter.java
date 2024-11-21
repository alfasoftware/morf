/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.dataset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.alfasoftware.morf.metadata.*;
import org.junit.Test;

/**
 * Ensure that {@link DataSetProducer}s can be augmented with meta data from
 * an alternate source.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestWithMetaDataAdapter {

  /**
   * Ensure that metadata can be added to an existing data set producer.
   */
  @Test
  public void testAddMetadata() {
    final MockDataProducer dataProducer = new MockDataProducer();
    final MockSchemaProducer schemaProducer = new MockSchemaProducer();
    final WithMetaDataAdapter adapter = new WithMetaDataAdapter(dataProducer, schemaProducer);

    // Check that open passes through correctly
    adapter.open();
    assertTrue("Data producer opened", dataProducer.openCalled);
    assertTrue("Schema producer opened", schemaProducer.openCalled);

    // Check that close passes through correctly
    adapter.close();
    assertTrue("Data producer closed", dataProducer.closeCalled);
    assertTrue("Schema producer closed", schemaProducer.closeCalled);

    // Check that the schema is correctly retrieved and passes through to
    // the correct target
    final Schema schema = adapter.getSchema();
    assertEquals("Schema retrieved correctly", MockDataProducer.class.getSimpleName(), schema.getTable("a").getName());
    assertEquals("Table names retreived from data set", MockDataProducer.class.getSimpleName(), schema.tableNames().toArray()[0].toString());
    assertFalse("isEmptyDatabase passes through to schema", schema.isEmptyDatabase());
    assertTrue("tableExists passes through to schema", schema.tableExists(MockSchemaProducer.class.getSimpleName()));
    assertEquals("tables passes through to data set", MockDataProducer.class.getSimpleName(), schema.tables().iterator().next().getName());

    // Check that the records are correctly pulled out
    final Iterable<Record> records = adapter.records("bob");
    final Record record = records.iterator().next();
    assertEquals("Version of record", 10L, record.getLong("version").longValue());
    assertEquals("ID of record", 1L, record.getLong("id").longValue());
    assertEquals("Random value from record", "Bob", record.getString("Alan"));
  }


  /**
   * Base mock producer - used for tracking method calls.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private abstract static class MockProducer implements DataSetProducer {
    /**
     * Tracks if open has been called.
     */
    protected boolean openCalled;

    /**
     * Tracks if close has been called.
     */
    protected boolean closeCalled;

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
     */
    @Override
    public void open() {
      openCalled = true;
    }

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
     */
    @Override
    public void close() {
      closeCalled = true;
    }

    /**
     * A fake schema.
     */
    private final Schema schema = new Schema() {
      /**
       * {@inheritDoc}
       *
       * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
       */
      @Override
      public Table getTable(String name) {
        return new Table() {

          /**
           * {@inheritDoc}
           * @see org.alfasoftware.morf.metadata.Table#isTemporary()
           */
          @Override
          public boolean isTemporary() {
            return false;
          }

          /**
           * {@inheritDoc}
           *
           * @see org.alfasoftware.morf.metadata.Table#columns()
           */
          @Override
          public java.util.List<Column> columns() {
            return null;
          }

          /**
           * {@inheritDoc}
           *
           * @see org.alfasoftware.morf.metadata.Table#getName()
           */
          @Override
          public String getName() {
            return MockDataProducer.class.getSimpleName();
          }

          /**
           * {@inheritDoc}
           *
           * @see org.alfasoftware.morf.metadata.Table#indexes()
           */
          @Override
          public java.util.List<Index> indexes() {
            return null;
          }

          @Override
          public boolean isPartitioned() { return false; }

          @Override
          public PartitioningRule partitioningRule() {
            //TODO: support metadata reading on whether the table is partitioned.
            return null;
          }

          ;
        };
      }

      /**
       * {@inheritDoc}
       *
       * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
       */
      @Override
      public boolean isEmptyDatabase() {
        return MockDataProducer.class.equals(MockProducer.this.getClass());
      }


      /**
       * {@inheritDoc}
       *
       * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
       */
      @Override
      public boolean tableExists(String name) {
        return tableNames().contains(name);
      }

      /**
       * {@inheritDoc}
       *
       * @see org.alfasoftware.morf.metadata.Schema#tables()
       */
      @Override
      public Collection<Table> tables() {
        Set<Table> tables = new HashSet<>();
        tables.add(getTable(""));
        return tables;
      }

      /**
       * {@inheritDoc}
       *
       * @see org.alfasoftware.morf.metadata.Schema#tableNames()
       */
      @Override
      public Collection<String> tableNames() {
        return Arrays.asList(MockProducer.this.getClass().getSimpleName());
      }

      @Override
      public boolean viewExists(String name) {
        return false;
      }

      @Override
      public View getView(String name) {
        return null;
      }

      @Override
      public Collection<String> viewNames() {
        return Collections.emptySet();
      }

      @Override
      public Collection<View> views() {
        return Collections.emptySet();
      }

      @Override
      public boolean sequenceExists(String name) {
        return false;
      }

      @Override
      public Sequence getSequence(String name) {
        return null;
      }

      @Override
      public Collection<String> sequenceNames() {
        return Collections.emptySet();
      }

      @Override
      public Collection<Sequence> sequences() {
        return Collections.emptySet();
      }
    };


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
     */
    @Override
    public Schema getSchema() {
      return schema;
    }


    /**
     * @see org.alfasoftware.morf.dataset.DataSetProducer#isTableEmpty(java.lang.String)
     */
    @Override
    public boolean isTableEmpty(String tableName) {
      return false;
    }
  }


  /**
   * A mock DataSetProducer that serves up a small amount of data.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static class MockDataProducer extends MockProducer {
    @Override
    public Iterable<Record> records(String tableName) {
      return Arrays.asList(DataSetUtils.record()
        .setLong("id", 1L)
        .setLong("version", 10L)
        .setString("Alan", "Bob")
      );
    }
  }


  /**
   * A mock data set producer that serves up a schema and nothing else.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static class MockSchemaProducer extends MockProducer {

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
     */
    @Override
    public Iterable<Record> records(String tableName) {
      throw new UnsupportedOperationException("Cannot get the records from the schema producer");
    }
  }
}
