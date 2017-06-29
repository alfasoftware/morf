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

import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;

import com.google.common.collect.Maps;

/**
 * DSL helpers for constructing a DataSet.
 * <p>Example usage (using static imports):</p>
 * <pre>
 *   DataSetProducer producer = dataSetProducer(schema)
 *     .table("SimpleTypes",
 *       record()
 *         .value("stringCol", "hello world")
 *         .value("decimalCol", "9817236")
 *       record()
 *         .value("stringCol", "hello world")
 *         .value("decimalCol", "9817236")
 *     );
 *
 * </pre>
 *
 */
public final class DataSetUtils {

  /**
   * Build a record.
   *
   * @see RecordBuilder
   * @return A {@link RecordBuilder}.
   */
  public static RecordBuilder record() {
    return new RecordBuilderImpl();
  }


  /**
   * Build a set of parameters for a single call of a parameterised SQL
   * statement.
   *
   * @see RecordBuilder
   * @return A {@link RecordBuilder}.
   */
  public static StatementParametersBuilder statementParameters() {
    return new StatementParametersBuilderImpl();
  }

  /**
   * Build a data set producer.
   *
   * @see DataSetProducerBuilder
   * @param schema The schema backing the dataset
   * @return A {@link DataSetProducerBuilder}.
   */
  public static DataSetProducerBuilder dataSetProducer(Schema schema) {
    return new DataSetProducerBuilderImpl(schema);
  }


  /**
   * Fluent interface for building a {@link DataValueLookup}.
   */
  public interface DataValueLookupBuilder extends DataValueLookup {

    /**
     * Specify a value for a particular column.
     *
     * @see Record
     * @param columnName The column name.
     * @param value The value in String (Record) format.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder value(String columnName, String value);
  }


  /**
   * Fluent interface for building a {@link Record}.
   */
  public interface RecordBuilder extends DataValueLookupBuilder, Record {

    /**
     * @see org.alfasoftware.morf.metadata.DataSetUtils.DataValueLookupBuilder#value(java.lang.String, java.lang.String)
     */
    @Override
    RecordBuilder value(String columnName, String value);
  }


  /**
   * Fluent interface for building a {@link StatementParameters}.
   */
  public interface StatementParametersBuilder extends DataValueLookupBuilder, StatementParameters {

    /**
     * @see org.alfasoftware.morf.metadata.DataSetUtils.DataValueLookupBuilder#value(java.lang.String, java.lang.String)
     */
    @Override
    StatementParametersBuilder value(String columnName, String value);
  }


  /**
   * Fluent interface for building a {@link DataSetProducer}.
   */
  public interface DataSetProducerBuilder extends DataSetProducer {

    /**
     * Specify a full table, including record data.
     * @param tableName The name of the table.
     * @param records The records for the table.
     * @return this, for method chaining.
     */
    DataSetProducerBuilder table(String tableName, List<Record> records);


    /**
     * Specify a full table, including record data.
     * @param tableName The name of the table.
     * @param records The records for the table.
     * @return this, for method chaining.
     */
    DataSetProducerBuilder table(String tableName, Record... records);
  }


  /**
   * Implements {@link DataSetProducerBuilder}.
   */
  private static class DataSetProducerBuilderImpl implements DataSetProducerBuilder {

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
    public void open() {}


    /**
     * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
     */
    @Override
    public void close() {}


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


  /**
   * Implements {@link DataValueLookupBuilder}.
   */
  private static class DataValueLookupBuilderImpl implements DataValueLookupBuilder {

    private final Map<String, String> values = Maps.newHashMap();

    /**
     * @see org.alfasoftware.morf.dataset.Record#getValue(java.lang.String)
     */
    @Override
    public String getValue(String name) {
      return values.get(name.toUpperCase());
    }


    /**
     * @see org.alfasoftware.morf.metadata.DataSetUtils.DataValueLookupBuilder#value(java.lang.String, java.lang.String)
     */
    @Override
    public DataValueLookupBuilder value(String columnName, String value) {
      values.put(columnName.toUpperCase(), value);
      return this;
    }


    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return values.toString();
    }
  }


  /**
   * Implements {@link RecordBuilder}.
   */
  private static class RecordBuilderImpl extends DataValueLookupBuilderImpl implements RecordBuilder {
    /**
     * @see org.alfasoftware.morf.metadata.DataSetUtils.DataValueLookupBuilderImpl#value(java.lang.String, java.lang.String)
     */
    @Override
    public RecordBuilder value(String columnName, String value) {
      return (RecordBuilder)super.value(columnName, value);
    }
  }


  /**
   * Implements {@link StatementParametersBuilder}.
   */
  private static class StatementParametersBuilderImpl extends DataValueLookupBuilderImpl implements StatementParametersBuilder {
    /**
     * @see org.alfasoftware.morf.metadata.DataSetUtils.DataValueLookupBuilderImpl#value(java.lang.String, java.lang.String)
     */
    @Override
    public StatementParametersBuilder value(String columnName, String value) {
      return (StatementParametersBuilder)super.value(columnName, value);
    }
  }
}
