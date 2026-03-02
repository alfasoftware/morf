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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;

/**
 * Adds meta data to a bunch of records.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class WithMetaDataAdapter extends DataSetProducerAdapter {

  /**
   * The producer of the schema.
   */
  private final DataSetProducer schemaProducer;

  /**
   * The schema extracted from the schema producer. This is lazy initialised
   * as the schema cannot be extracted on construction (the producer might not
   * be open yet).
   */
  private Schema schema;


  /**
   * Creates the adapter with the two given sources.
   *
   * @param dataProducer producer of the data
   * @param schemaProducer producer of the schema
   */
  public WithMetaDataAdapter(DataSetProducer dataProducer, DataSetProducer schemaProducer) {
    super(dataProducer);
    this.schemaProducer = schemaProducer;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
   */
  @Override
  public void close() {
    super.close();
    schemaProducer.close();
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
   */
  @Override
  public Schema getSchema() {
    final Schema targetSchema = schemaProducer.getSchema();
    final Schema sourceSchema = super.getSchema();
    if (schema == null) {
      schema = new Schema() {
        @Override
        public Table getTable(String name) {
          return targetSchema.getTable(name);
        }

        @Override
        public boolean isEmptyDatabase() {
          return targetSchema.isEmptyDatabase();
        }

        @Override
        public boolean tableExists(String name) {
          return targetSchema.tableExists(name);
        }

        @Override
        public Collection<String> tableNames() {
          return sourceSchema.tableNames();
        }

        @Override
        public Collection<Table> tables() {
          Set<Table> tables = new HashSet<>();
          for (String tableName : tableNames()) {
            tables.add(getTable(tableName));
          }
          return tables;
        }

        @Override
        public boolean viewExists(String name) {
          return targetSchema.viewExists(name);
        }

        @Override
        public View getView(String name) {
          return targetSchema.getView(name);
        }

        @Override
        public Collection<String> viewNames() {
          return targetSchema.viewNames();
        }

        @Override
        public Collection<View> views() {
          return targetSchema.views();
        }

        @Override
        public boolean sequenceExists(String name) {
          return targetSchema.sequenceExists(name);
        }

        @Override
        public Sequence getSequence(String name) {
          return targetSchema.getSequence(name);
        }

        @Override
        public Collection<String> sequenceNames() {
          return targetSchema.sequenceNames();
        }

        @Override
        public Collection<Sequence> sequences() {
          return targetSchema.sequences();
        }
      };
    }
    return schema;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
   */
  @Override
  public void open() {
    super.open();
    schemaProducer.open();
  }
}
