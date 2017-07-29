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

import static org.alfasoftware.morf.metadata.DataType.BIG_INTEGER;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.sql.SelectStatement;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

/**
 * Utility functions for Schemas.
 * <p>
 * The utility methods within this class afford the methods required to create
 * representations of the following objects:
 * </p>
 * <ul>
 * <li>{@link Schema}</li>
 * <li>{@link Table}</li>
 * <li>{@link View}</li>
 * <li>{@link Column}</li>
 * <li>{@link Index}</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public final class SchemaUtils {

  /**
   * Prevent construction.
   */
  private SchemaUtils() {
  };


  /**
   * Creates a Column that defines the standard ID column.
   *
   * @return an ID Column.
   */
  public static Column idColumn() {
    return new ColumnBean("id", DataType.BIG_INTEGER, 19, 0, false, "", true);
  }


  /**
   * Creates a Column that defines the standard version column.
   *
   * @return a version Column.
   */
  public static Column versionColumn() {
    return new ColumnBean("version", DataType.INTEGER, 10, 0, true, "0");
  }


  /**
   * Build an empty schema.
   *
   * @return Build an empty schema.
   */
  public static Schema schema() {
    return new SchemaBean();
  }


  /**
   * Build a {@link Schema} from a list of {@link Table}s.
   *
   * @param tables The tables to use.
   * @return A {@link Schema} implementation
   */
  public static Schema schema(Table... tables) {
    return new SchemaBean(tables);
  }


  /**
   * Build a {@link Schema} from a list of {@link Table}s.
   *
   * @param views The views to use.
   * @return A {@link Schema} implementation
   */
  public static Schema schema(View... views) {
    return new SchemaBean(views);
  }


  /**
   * Build a {@link Schema} from a list of {@link Table}s.
   *
   * @param tables The tables to use.
   * @return A {@link Schema} implementation
   */
  public static Schema schema(Iterable<Table> tables) {
    return new SchemaBean(tables);
  }


  /**
   * Build a {@link Schema} from a list of {@link View}s.
   *
   * @param views The views to use.
   * @return A {@link Schema} implementation
   */
  public static Schema schema(Collection<View> views) {
    return new SchemaBean(Collections.<Table> emptySet(), views);
  }


  /**
   * Build a {@link Schema} from a list of schema. The resulting schema is the
   * superset of all elements.
   *
   * @param schema Schema to combine.
   * @return A single schema representing all of {@code schema}.
   */
  public static Schema schema(Schema... schema) {
    return new CompositeSchema(schema);
  }


  /**
   * Copy a {@link Schema}.
   *
   * @param schema The schema to copy.
   * @return A {@link Schema} implementation copied from the provided
   *         {@link Schema}.
   */
  public static Schema copy(Schema schema) {
    return new SchemaBean(schema);
  }


  /**
   * Build a {@link Table}.
   * <p>
   * Use {@link TableBuilder#columns(Column...)} to provide columns and
   * {@link TableBuilder#indexes(Index...)} to provide indexes.
   * </p>
   *
   * @param name The name of the table.
   * @return A new {@link TableBuilder} for the table.
   */
  public static TableBuilder table(String name) {
    return new TableBuilderImpl(name);
  }


  /**
   * Copy a {@link Table}
   *
   * @param table The table to copy.
   * @return A {@link Table} implementation copied from the provided
   *         {@link Table}.
   */
  public static Table copy(Table table) {
    return new TableBean(table);
  }


  /**
   * Copy an {@link Index}
   *
   * @param index The index to copy.
   * @return A {@link Index} implementation copied from the provided
   *         {@link Index}.
   */
  public static Index copy(Index index) {
    return new IndexBean(index);
  }


  /**
   * Build a {@link Column}.
   * <p>
   * Use the methods on {@link ColumnBuilder} to provide optional properties.
   * </p>
   *
   * @param name The column name.
   * @param type The column type.
   * @param width The column width
   * @param scale The column scale.
   * @return A new {@link ColumnBuilder} for the column.
   */
  public static ColumnBuilder column(String name, DataType type, int width, int scale) {
    return new ColumnBuilderImpl(name, type, width, scale);
  }


  /**
   * Build a {@link Column}.
   * <p>
   * This method defaults the scale to zero.
   * </p>
   * <p>
   * Use the methods on {@link ColumnBuilder} to provide optional properties.
   * </p>
   *
   * @param name The column name.
   * @param type The column type.
   * @param width The column width
   * @return A new {@link ColumnBuilder} for the column.
   */
  public static ColumnBuilder column(String name, DataType type, int width) {
    return new ColumnBuilderImpl(name, type, width, 0);
  }


  /**
   * Build a {@link Column}.
   * <p>
   * This method defaults the scale and width to zero, for data types where this
   * is not relevant.
   * </p>
   * <p>
   * Use the methods on {@link ColumnBuilder} to provide optional properties.
   * </p>
   *
   * @param name The column name.
   * @param type The column type.
   * @return A new {@link ColumnBuilder} for the column.
   */
  public static ColumnBuilder column(String name, DataType type) {
    return new ColumnBuilderImpl(name, type, 0, 0);
  }


  /**
   * Build a {@link Column}.
   * <p>
   * This method returns a {@link ColumnBuilder} whose properties are an exact copy of the passed in {@link Column}.
   * </p>
   * <p>
   * Use the methods on {@link ColumnBuilder} to provide optional properties.
   * </p>
   *
   * @param column The column to copy.
   * @return A new {@link ColumnBuilder} for the column.
   */
  public static ColumnBuilder column(Column column) {
    return new ColumnBuilderImpl(column, column.isNullable(), column.getDefaultValue(), column.isPrimaryKey(), column.isAutoNumbered(), column.getAutoNumberStart());
  }


  /**
   * Build an auto-increment primary key column.  Type defaults to a universally
   * safe big integer.  No further modification is possible.
   *
   * @param name The column name.
   * @param startFrom The auto-increment start value.
   * @return A new {@link Column}.
   */
  public static Column autonumber(String name, int startFrom) {
    return new ColumnBean(name, BIG_INTEGER, 0, 0, false, null, true, true, startFrom);
  }


  /**
   * Build an index.
   * <p>
   * Use the methods on {@link IndexBuilder} to provide columns and optional
   * properties.
   * </p>
   *
   * @param name The name of the index.
   * @return An {@link IndexBuilder} for the index.
   */
  public static IndexBuilder index(String name) {
    return new IndexBuilderImpl(name);
  }


  /**
   * Create a view.
   *
   * @param viewName The name of the view.
   * @param selectStatement The underlying {@link SelectStatement}. This can be null e.g. if loading from database metadata or in testing.
   * @param dependencies names of any views that this view depends on (and therefore need to be deployed first).
   * @return {@link View} implementation based on the parameters provided.
   */
  public static View view(String viewName, SelectStatement selectStatement, String... dependencies) {
    return new ViewBean(viewName, selectStatement, dependencies);
  }


  /**
   * Create a copy of a view.
   *
   * @param view The {@link View} to copy.
   * @return {@link View} implementation copied from the provided view.
   */
  public static View copy(View view) {
    return new ViewBean(view);
  }


  /**
   * Create a column type.
   *
   * @param type The {@link DataType} of the field.
   * @return The {@link ColumnType}.
   */
  public static ColumnType type(DataType type) {
    return SchemaUtils.column(null, type);
  }


  /**
   * Create a column type.
   *
   * @param type The {@link DataType} of the field.
   * @param width The field width.
   * @return The {@link ColumnType}.
   */
  public static ColumnType type(DataType type, int width) {
    return SchemaUtils.column(null, type, width);
  }


  /**
   * Create a column type.
   *
   * @param type The {@link DataType} of the field.
   * @param width The field width.
   * @param scale The field scale.
   * @return The {@link ColumnType}.
   */
  public static ColumnType type(DataType type, int width, int scale) {
    return SchemaUtils.column(null, type, width, scale);
  }


  /**
   * Create a column type.
   *
   * @param type The {@link DataType} of the field.
   * @param width The field width.
   * @param scale The field scale.
   * @param nullable Whether the field should be nullable.
   * @return The {@link ColumnType}.
   */
  public static ColumnType type(DataType type, int width, int scale, boolean nullable) {
    ColumnBuilder builder = SchemaUtils.column(null, type, width, scale);
    if (nullable) {
      builder = builder.nullable();
    }
    return builder;
  }


  /**
   * Builds {@link Table} implementation.
   */
  public interface TableBuilder extends Table {

    /**
     * Sets the columns for the table.
     *
     * @param columns The columns to set, probably provided by calls to
     *          {@link SchemaUtils#column(String, DataType, int, int)}.
     * @return this table builder, for method chaining.
     */
    public TableBuilder columns(Column... columns);


    /**
     * Sets the indexes for the table.
     *
     * @param indexes The indexes to set, probably provided by calls to
     *          {@link SchemaUtils#index(String)}
     * @return this table builder, for method chaining.
     */
    public TableBuilder indexes(Index... indexes);


    /**
     * Creates a temporary table.
     *
     * @return this table builder, for method chaining.
     */
    public TableBuilder temporary();
  }

  /**
   * Builds {@link Column} implementations.
   */
  public interface ColumnBuilder extends Column {

    /**
     * Mark the column as nullable.
     *
     * @return this, for method chaining.
     */
    public ColumnBuilder nullable();


    /**
     * Sets the default value for the column.
     * <p>
     * This method should only be used for temporary column definitions. There
     * is no mechanism to declare a default value in the domain layer, meaning
     * that any defaults specified here will result in a schema mismatch.
     * </p>
     *
     * @param value The default value to set.
     * @return this, for method chaining.
     */
    public ColumnBuilder defaultValue(String value);


    /**
     * Mark this column as part of the primary key for the table.
     *
     * @return this, for method chaining.
     */
    public ColumnBuilder primaryKey();


    /**
     * Mark this column as not part of the primary key for the table.
     *
     * @return this, for method chaining.
     */
    public ColumnBuilder notPrimaryKey();


    /**
     * Mark this column as autonumbered, with the specified starting value
     * @param from the starting value
     *
     * @return this, for method chaining.
     */
    public ColumnBuilder autoNumbered(int from);
  }

  /**
   * Builds {@link Index} implementations.
   */
  public interface IndexBuilder extends Index {

    /**
     * Set the column names for the index.
     *
     * @param columnNames The column names to set.
     * @return this, for method chaining.
     */
    public IndexBuilder columns(String... columnNames);


    /**
     * Set the column names for the index.
     *
     * @param columnNames The column names to set.
     * @return this, for method chaining.
     */
    public IndexBuilder columns(Iterable<String> columnNames);


    /**
     * Mark this index as unique.
     *
     * @return this, for method chaining.
     */
    public IndexBuilder unique();
  }

  /**
   * Private implementation of {@link TableBuilder}.
   */
  private static final class TableBuilderImpl extends TableBean implements TableBuilder {

    private TableBuilderImpl(String name) {
      super(name);
    }


    private TableBuilderImpl(String name, List<Column> columns, List<Index> indexes, boolean isTemporary) {
      super(name, columns, indexes, isTemporary);
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder#columns(org.alfasoftware.morf.metadata.Column[])
     */
    @Override
    public TableBuilder columns(Column... columns) {
      return new TableBuilderImpl(getName(), Arrays.asList(columns), indexes(), isTemporary());
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder#indexes(org.alfasoftware.morf.metadata.Index[])
     */
    @Override
    public TableBuilder indexes(Index... indexes) {
      return new TableBuilderImpl(getName(), columns(), Arrays.asList(indexes), isTemporary());
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder#temporary()
     */
    @Override
    public TableBuilder temporary() {
      return new TableBuilderImpl(getName(), columns(), indexes(), true);
    }
  }

  /**
   * Private implementation of {@link ColumnBuilder}.
   */
  private static final class ColumnBuilderImpl extends ColumnBean implements ColumnBuilder {

    private ColumnBuilderImpl(String name, DataType type, int width, int scale) {
      super(name, type, width, scale, false, "", false);
    }


    private ColumnBuilderImpl(Column toCopy, boolean nullable, String defaultValue, boolean primaryKey, boolean autoNumbered, int numberedFrom) {
      super(toCopy.getName(), toCopy.getType(), toCopy.getWidth(), toCopy.getScale(), nullable, defaultValue, primaryKey, autoNumbered, numberedFrom);
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder#nullable()
     */
    @Override
    public ColumnBuilder nullable() {
      return new ColumnBuilderImpl(this, true, getDefaultValue(), isPrimaryKey(), isAutoNumbered(), getAutoNumberStart());
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder#defaultValue(java.lang.String)
     */
    @Override
    public ColumnBuilder defaultValue(String value) {
      return new ColumnBuilderImpl(this, isNullable(), value, isPrimaryKey(), isAutoNumbered(), getAutoNumberStart());
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder#primaryKey()
     */
    @Override
    public ColumnBuilder primaryKey() {
      return new ColumnBuilderImpl(this, isNullable(), getDefaultValue(), true, isAutoNumbered(), getAutoNumberStart());
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder#autoNumbered(int)
     */
    @Override
    public ColumnBuilder autoNumbered(int from) {
      return new ColumnBuilderImpl(this, isNullable(), getDefaultValue(), isPrimaryKey(), true, from);
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder#notPrimaryKey()
     */
    @Override
    public ColumnBuilder notPrimaryKey() {
      return new ColumnBuilderImpl(this, isNullable(), getDefaultValue(), false, isAutoNumbered(), getAutoNumberStart());
    }
  }

  /**
   * Private implementation of {@link IndexBuilder}.
   */
  private static final class IndexBuilderImpl extends IndexBean implements IndexBuilder {

    private IndexBuilderImpl(String name) {
      super(name, false, new String[0]);
    }


    private IndexBuilderImpl(String name, boolean unique, Iterable<String> columnNames) {
      super(name, unique, columnNames);
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder#columns(java.lang.String[])
     */
    @Override
    public IndexBuilder columns(String... columnNames) {
      return new IndexBuilderImpl(getName(), isUnique(), Arrays.asList(columnNames));
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder#columns(java.lang.Iterable)
     */
    @Override
    public IndexBuilder columns(Iterable<String> columnNames) {
      return new IndexBuilderImpl(getName(), isUnique(), columnNames);
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder#unique()
     */
    @Override
    public IndexBuilder unique() {
      return new IndexBuilderImpl(getName(), true, columnNames());
    }

  }


  /**
   * List the primary key columns for a given table.
   *
   * @param table The table
   * @return The primary keys
   */
  public static List<Column> primaryKeysForTable(Table table) {
    List<Column> result = Lists.newArrayList();
    for (Column column : table.columns()) {
      if (column.isPrimaryKey()) {
        result.add(column);
      }
    }
    return result;
  }


  /**
   * Turn a list of columns into a list of the columns' names.
   *
   * @param columns The columns
   * @return The columns' names.
   */
  public static List<String> namesOfColumns(List<Column> columns) {
    return Lists.transform(columns, new Function<Column, String>() {
      @Override
      public String apply(Column column) {
        return column.getName();
      }
    });
  }


  /**
   * Convert all the strings in a list to upper case.
   *
   * @param listOfStrings A list of strings
   * @return A new list of strings, with each string converted to upper case
   */
  public static List<String> toUpperCase(List<String> listOfStrings) {
    return FluentIterable.from(listOfStrings).transform(new Function<String, String>() {
      @Override
      public String apply(String value) {
        return value.toUpperCase();
      }
    }).toList();
  }
}
