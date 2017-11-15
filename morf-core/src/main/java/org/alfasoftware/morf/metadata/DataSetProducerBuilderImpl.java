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

package org.alfasoftware.morf.metadata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.DataSetUtils.DataSetProducerBuilder;

import com.google.common.collect.Maps;

/**
 * Implements {@link DataSetProducerBuilder}.
 */
class DataSetProducerBuilderImpl implements DataSetProducerBuilder {

  private final Schema schema;
  private final Map<String, List<Record>> recordMap = Maps.newHashMap();

  /**
   * @param schema
   */
  public DataSetProducerBuilderImpl(Schema schema) {
    super();
    this.schema = schema;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
   */
  @Override
  public void open() {
    // Nothing to do
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
   */
  @Override
  public void close() {
    // Nothing to do
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
   */
  @Override
  public Schema getSchema() {
    return schema;
  }


  /**
   * @see org.alfasoftware.morf.metadata.DataSetUtils.DataSetProducerBuilder#table(java.lang.String, java.util.List)
   */
  @Override
  public DataSetProducerBuilder table(String tableName, List<Record> records) {
    recordMap.put(tableName.toUpperCase(), records);
    return this;
  }


  /**
   * @see org.alfasoftware.morf.metadata.DataSetUtils.DataSetProducerBuilder#table(java.lang.String, java.util.List)
   */
  @Override
  public DataSetProducerBuilder table(String tableName, Record... records) {
    table(tableName, Arrays.asList(records));
    return this;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
   */
  @Override
  public List<Record> records(String tableName) {
    List<Record> records = recordMap.get(tableName.toUpperCase());
    if (records == null) {
      throw new IllegalStateException("No record data has been provided for table [" + tableName + "]");
    }
    return records;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#isTableEmpty(java.lang.String)
   */
  @Override
  public boolean isTableEmpty(String tableName) {
    return records(tableName).isEmpty();
  }
}
