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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.List;
import java.util.function.BiFunction;

import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.joda.time.LocalDate;

import com.google.inject.spi.TypeConverter;


/**
 * DSL helpers for constructing a DataSet.
 * <p>Example usage (using static imports):</p>
 * <pre>
 *   DataSetProducer producer = dataSetProducer(schema)
 *     .table("SimpleTypes",
 *       record()
 *         .setString("stringCol", "hello world")
 *         .setInteger("decimalCol", 9817236)
 *       record()
 *         .setString("stringCol", "hello world")
 *         .setInteger("decimalCol", 9817236)
 *     );</pre>
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
     * Hints to the builder how many columns you are likely to need,
     * allowing it to size the storage array appropriately and avoid
     * resizing.  Any additional column values specified beyond this
     * will trigger a resize.
     *
     * @param count The column count.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder withInitialColumnCount(int count);

    /**
     * Specify a value for a particular column.
     *
     * @see Record
     * @param columnName The column name.
     * @param value The value in String (Record) format.
     * @return this, for method chaining.
     * @deprecated Use the appropriate setter method for your type (e.g.
     *            {@link #setBoolean(String, Boolean)} or
     *            {@link #setBigDecimal(String, BigDecimal)}).  Avoid using
     *            {@link #setString(String, String)} for all cases where
     *            {@link #value(String, String)} is currently used (despite
     *            the fact that this will, in fact, work) since if you are
     *            storing a non string type, you will be forcing the code
     *            to perform a conversion from a string to your target
     *            type later.
     */
    @Deprecated
    DataValueLookupBuilder value(String columnName, String value);

    /**
     * Specify a value for a particular column. If the string actually contains
     * typed data such as "123" or "2017-12-31" (e.g. from XML), then conversion will be
     * handled automatically when the record is read later using typed getter
     * methods (in those cases, {@link #getInteger(String)} or {@link #getDate(String)}).
     *
     * @param columnName The column name.
     * @param value The value.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setString(String columnName, String value);

    /**
     * Specify a value for a particular column.
     *
     * @param columnName The column name.
     * @param value The value.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setInteger(String columnName, Integer value);

    /**
     * Specify a value for a particular column.
     *
     * @param columnName The column name.
     * @param value The value.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setLong(String columnName, Long value);

    /**
     * Specify a value for a particular column.
     *
     * @param columnName The column name.
     * @param value The value.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setBoolean(String columnName, Boolean value);

    /**
     * Specify a value for a particular column as a {@link org.joda.time.LocalDate}.
     * Note that it may be freely read as a string or {@link java.sql.Date}.
     *
     * @param columnName The column name.
     * @param value The value,
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setLocalDate(String columnName, org.joda.time.LocalDate value);

    /**
     * Specify a value for a particular column as a {@link java.sql.Date}.
     * Note that it may be freely read as a string or {@link org.joda.time.LocalDate}.
     *
     * @param columnName The column name.
     * @param value The value,
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setDate(String columnName, java.sql.Date value);

    /**
     * Specify a value for a particular column as a double-width floating point
     * integer. Be wary of loss of precision: see {@link DataValueLookup#getDouble(String)}
     * and avoid using this for financial amounts.
     *
     * @param columnName The column name.
     * @param value The value.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setDouble(String columnName, Double value);

    /**
     * Specify a value for a particular column.
     *
     * @param columnName The column name.
     * @param value The value.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setBigDecimal(String columnName, BigDecimal value);

    /**
     * Specify a value for a particular column as binary data.  See
     * {@link DataValueLookup#getByteArray(String)} and be aware of Base 64
     * conversion if the value is subsequently read as a string.
     *
     * <p><strong>Note</strong> that for safety, the byte array is copied
     * when set (to avoid being corrupted by the producer) and also copied
     * when read in {@link #getByteArray(String)} (to avoid being corrupted
     * by the consumer).  If this turns out to be too inefficient, an
     * alternative unsafe method could be created.</p>
     *
     * @param columnName The column name.
     * @param value The value.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setByteArray(String columnName, byte[] value);

    /**
     * Sets a column value explicitly to null.
     *
     * @param columnName The column name.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setNull(String columnName);

    /**
     * Takes either a {@link String}, {@link Integer}, {@link Long},
     * {@link BigDecimal}, {@link Boolean}, {@link Double}, {@link java.sql.Date},
     * {@link org.joda.time.LocalDate} or byte array and calls the appropriate
     * typed method (e.g. {@link #setBoolean(String, Boolean)}).
     *
     * <p>Useful for interacting with {@link ResultSet}.</p>
     *
     * @param columnName The column name.
     * @param value The value.
     * @return this, for method chaining.
     */
    DataValueLookupBuilder setObject(String columnName, Object value);
  }


  /**
   * Fluent interface for building a {@link Record}.
   */
  public interface RecordBuilder extends DataValueLookupBuilder, Record {
    @Override
    RecordBuilder withInitialColumnCount(int count);
    @Override
    @Deprecated
    RecordBuilder value(String columnName, String value);
    @Override
    RecordBuilder setObject(String columnName, Object value);
    @Override
    RecordBuilder setString(String columnName, String value);
    @Override
    RecordBuilder setInteger(String columnName, Integer value);
    @Override
    RecordBuilder setLong(String columnName, Long value);
    @Override
    RecordBuilder setBoolean(String columnName, Boolean value);
    @Override
    RecordBuilder setLocalDate(String columnName, LocalDate value);
    @Override
    RecordBuilder setDate(String columnName, java.sql.Date value);
    @Override
    RecordBuilder setDouble(String columnName, Double value);
    @Override
    RecordBuilder setBigDecimal(String columnName, BigDecimal value);
    @Override
    RecordBuilder setByteArray(String columnName, byte[] value);
  }


  /**
   * Fluent interface for building a {@link StatementParameters}.
   */
  public interface StatementParametersBuilder extends DataValueLookupBuilder, StatementParameters {
    @Override
    StatementParametersBuilder withInitialColumnCount(int count);
    @Override
    @Deprecated
    StatementParametersBuilder value(String columnName, String value);
    @Override
    StatementParametersBuilder setObject(String columnName, Object value);
    @Override
    StatementParametersBuilder setString(String columnName, String value);
    @Override
    StatementParametersBuilder setInteger(String columnName, Integer value);
    @Override
    StatementParametersBuilder setLong(String columnName, Long value);
    @Override
    StatementParametersBuilder setBoolean(String columnName, Boolean value);
    @Override
    StatementParametersBuilder setLocalDate(String columnName, LocalDate value);
    @Override
    StatementParametersBuilder setDate(String columnName, java.sql.Date value);
    @Override
    StatementParametersBuilder setDouble(String columnName, Double value);
    @Override
    StatementParametersBuilder setBigDecimal(String columnName, BigDecimal value);
    @Override
    StatementParametersBuilder setByteArray(String columnName, byte[] value);
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
   * Makes lambdas a bit clearer in purpose. Describes a {@link BiFunction}
   * which takes a stored value and a corresponding {@link ValueConverter} and
   * produces a required output type.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   * @param <STORED> The type of the stored value.
   * @param <RETURNED> The type of the returned value.
   */
  interface ValueMapper<STORED, RETURNED> {

    /**
     * Takes a stored value and converter and returns the intended
     * typed return value.
     *
     * @param value The value.
     * @param converter The {@link TypeConverter}.
     * @return The converted value.
     */
    RETURNED map(STORED value, ValueConverter<STORED> converter);

    /** Lambdas struggle with introspecting primitive arrays as generic parameters, so we do this
     * the old fashioned way.
     */
    static final ValueMapper<Object, byte[]> OBJECT_TO_BYTE_ARRAY = new ValueMapper<Object, byte[]>() {
      @Override
      public byte[] map(Object o, ValueConverter<Object> c) {
        return c.byteArrayValue(o);
      }
    };
  }


  /**
   * Takes an existing record and adds additional or override values without
   * copying or modifying the existing record, minimising the need for additional
   * memory.
   */
  public static class RecordDecorator extends RecordBuilderImpl {

    private final Record fallback;

    /**
     * Creates a new record decorator, which initially contains the values in
     * the fallback record, but allows values to be added or overridden.
     *
     * @param fallback The record to override.
     * @return A new {@link RecordBuilder}.
     */
    public static RecordBuilder of(Record fallback) {
      return new RecordDecorator(fallback);
    }

    protected RecordDecorator(Record fallback) {
      super();
      this.fallback = fallback;
    }

    @SuppressWarnings("deprecation")
    @Override
    public String getValue(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.stringValue(o)) : fallback.getValue(name);
    }

    @Override
    public BigDecimal getBigDecimal(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.bigDecimalValue(o)) : fallback.getBigDecimal(name);
    }

    @Override
    public Boolean getBoolean(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.booleanValue(o)) : fallback.getBoolean(name);
    }

    @Override
    public java.sql.Date getDate(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.dateValue(o)) : fallback.getDate(name);
    }

    @Override
    public Double getDouble(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.doubleValue(o)) : fallback.getDouble(name);
    }

    @Override
    public Integer getInteger(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.integerValue(o)) : fallback.getInteger(name);
    }

    @Override
    public LocalDate getLocalDate(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.localDateValue(o)) : fallback.getLocalDate(name);
    }

    @Override
    public Long getLong(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.longValue(o)) : fallback.getLong(name);
    }

    @Override
    public byte[] getByteArray(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, ValueMapper.OBJECT_TO_BYTE_ARRAY) : fallback.getByteArray(name);
    }

    @Override
    public Object getObject(Column column) {
      int index = indexOf(column.getName());
      return index > -1 ? super.getObject(column) : fallback.getObject(column);
    }

    @Override
    public String getString(String name) {
      int index = indexOf(name);
      return index > -1 ? getAndConvertByIndex(index, (o, c) -> c.stringValue(o)) : fallback.getString(name);
    }

    @Override
    public String toString() {
      return fallback.toString() + " + " + super.toString();
    }
  }
}