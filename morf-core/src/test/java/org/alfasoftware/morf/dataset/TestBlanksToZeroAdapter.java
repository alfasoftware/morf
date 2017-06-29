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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;

/**
 * Ensure that the {@link BlanksToZeroAdapter} adapts blank values correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestBlanksToZeroAdapter {

  /**
   * Ensure that the records retrieved are wrapped correctly.
   */
  @Test
  public void testRecords() {
    DataSetProducer producer = new MockDataProducer();
    BlanksToZeroAdapter adapter = new BlanksToZeroAdapter(producer);

    final Iterator<Record> records = adapter.records("Bob").iterator();

    final Record noneBlank = records.next();
    assertEquals("None blank string unchanged", "NotBlank", noneBlank.getValue("String"));
    assertEquals("None blank numbers unchanged", "1", noneBlank.getValue("Number"));

    final Record blank = records.next();
    assertEquals("Blank string unchanged", "", blank.getValue("String"));
    assertEquals("Blank numbers set to zero", "0", blank.getValue("Number"));
  }


  /**
   * A mock data producer
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static class MockDataProducer implements DataSetProducer {

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
     */
    @Override
    public void close() {
    }

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
     */
    @Override
    public Schema getSchema() {
      return new Schema() {
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
            public List<Column> columns() {
              return Arrays.<Column> asList(
                column("String", DataType.STRING),
                column("Number", DataType.DECIMAL)
              );
            }

            /**
             * {@inheritDoc}
             *
             * @see org.alfasoftware.morf.metadata.Table#getName()
             */
            @Override
            public String getName() {
              return null;
            }

            /**
             * {@inheritDoc}
             *
             * @see org.alfasoftware.morf.metadata.Table#indexes()
             */
            @Override
            public List<Index> indexes() {
              return null;
            }
          };
        }

        /**
         * {@inheritDoc}
         *
         * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
         */
        @Override
        public boolean isEmptyDatabase() {
          return false;
        }

        /**
         * {@inheritDoc}
         *
         * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
         */
        @Override
        public boolean tableExists(String name) {
          return false;
        }

        /**
         * {@inheritDoc}
         *
         * @see org.alfasoftware.morf.metadata.Schema#tableNames()
         */
        @Override
        public Collection<String> tableNames() {
          return null;
        }

        /**
         * {@inheritDoc}
         *
         * @see org.alfasoftware.morf.metadata.Schema#tables()
         */
        @Override
        public Collection<Table> tables() {
          return null;
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
      };
    }

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
     */
    @Override
    public void open() {
    }

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
     */
    @Override
    public Iterable<Record> records(String tableName) {
      return Arrays.asList(
        (Record)new MockRecord("String", "NotBlank", "Number", "1"),
        (Record)new MockRecord("String", "", "Number", "")
      );
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
   * A mock record that just provides a key-value store.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static class MockRecord implements Record {
    /**
     * The values for this record.
     */
    private final Map<String, String> values;

    /**
     * Creates the mock record with the given key/value pairs
     * @param keyValuePairs ...
     */
    public MockRecord(String... keyValuePairs) {
      values = new HashMap<>();
      for (int i = 0; i < keyValuePairs.length; i += 2) {
        values.put(keyValuePairs[i], keyValuePairs[i + 1]);
      }
    }

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.dataset.Record#getValue(java.lang.String)
     */
    @Override
    public String getValue(String name) {
      return values.get(name);
    }
  }
}
