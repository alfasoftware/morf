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

package org.alfasoftware.morf.jdbc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

/**
 * Adds field defaults for columns that are missing from an insert statement.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class InsertStatementDefaulter {

  /**
   * The database schema.
   */
  private final Schema metadata;


  /**
   * Constructor.
   *
   * @param metadata the database schema.
   */
  protected InsertStatementDefaulter(Schema metadata) {
    super();
    this.metadata = metadata;

    if (metadata == null) {
      throw new IllegalArgumentException("MetaData must be provided");
    }
  }


  /**
   * Inserts default values for missing fields into the insert statement.
   *
   * @param statement the {@link InsertStatement} to add defaults for.
   * @return an insert statement with appropriate defaults added.
   */
  public InsertStatement defaultMissingFields(InsertStatement statement) {
    // Don't fiddle with parameterised statements
    if (statement.isParameterisedInsert()) {
      return statement;
    }

    Set<String> columnsWithValues = getColumnsWithValues(statement);
    return addColumnDefaults(statement, columnsWithValues);
  }


  /**
   * Gets a set of columns for which values have been provided.
   *
   * @param statement the statement to parse.
   * @return a set of columns for which values have been provided.
   */
  private Set<String> getColumnsWithValues(InsertStatement statement) {
    Set<String> columnsWithValues = new HashSet<>();

    addColumns(statement.getValues(), columnsWithValues);

    if (statement.getSelectStatement() != null) {
      addColumns(statement.getSelectStatement().getFields(), columnsWithValues);
    }

    if (statement.getFromTable() != null) {
      addColumnsFromSchema(statement.getFromTable(), columnsWithValues);
    }

    for (String columnName : statement.getFieldDefaults().keySet()) {
      columnsWithValues.add(columnName.toUpperCase());
    }

    return columnsWithValues;
  }


  /**
   * Adds the list of {@code fields} to the {@code columnsWithValues}.
   *
   * @param fields the fields to add.
   * @param columnsWithValues the set to add to.
   */
  private void addColumns(List<AliasedField> fields, Set<String> columnsWithValues) {
    for (AliasedField field : fields) {
      columnsWithValues.add(field.getAlias().toUpperCase());
    }
  }


  /**
   * Adds table columns from the schema.
   *
   * @param tableReference the table reference to add for.
   * @param columnsWithValues the set to add to.
   */
  private void addColumnsFromSchema(TableReference tableReference, Set<String> columnsWithValues) {
    Table table = metadata.getTable(tableReference.getName().toUpperCase());
    if (table == null) {
      throw new IllegalArgumentException("Could not find table in schema for: " + tableReference.getName());
    }

    for (Column column : table.columns()) {
      columnsWithValues.add(column.getName().toUpperCase());
    }
  }


  /**
   * Adds the column defaults for missing columns to the {@code statement}.
   *
   * @param statement the statement to add to.
   * @param columnsWithValues the columns for which we have values.
   */
  private InsertStatement addColumnDefaults(InsertStatement statement, Set<String> columnsWithValues) {
    Table table = metadata.getTable(statement.getTable().getName().toUpperCase());
    if (table == null) {
      throw new IllegalArgumentException("Could not find table in schema for: " + statement.getTable().getName());
    }

    List<AliasedFieldBuilder> aliasedFieldBuilders = Lists.newArrayList();
    for (Column currentColumn : table.columns()) {
      // Default date columns to null and skip columns we've already added.
      if (columnsWithValues.contains(currentColumn.getName().toUpperCase())) {
        continue;
      }

      AliasedField fieldDefault = getFieldDefault(currentColumn);
      if (fieldDefault == null) {
        continue;
      }

      if(AliasedField.immutableDslEnabled()) {
        aliasedFieldBuilders.add(fieldDefault.as(currentColumn.getName()));
      }
      else {
        statement.getFieldDefaults().put(currentColumn.getName(), fieldDefault);
      }
    }
    if(AliasedField.immutableDslEnabled()) {
      return statement.shallowCopy().withDefaults(aliasedFieldBuilders).build();
    }
    return statement;
  }


  /**
   * Gets the default value for the {@code column}. If the column has a default
   * value associated with it, this is returned. Otherwise a standard default is
   * given.
   *
   * @param column The column to get the default for.
   * @return the default value to use.
   */
  private AliasedField getFieldDefault(Column column) {
    if (isNullDefaultType(column)) {
      return new NullFieldLiteral().as(column.getName());
    }

    if (StringUtils.isNotEmpty(column.getDefaultValue())) {
      switch (column.getType()) {
        case STRING:
          return new FieldLiteral(column.getDefaultValue()).as(column.getName());
        case BOOLEAN:
          return new FieldLiteral(Boolean.valueOf(column.getDefaultValue())).as(column.getName());
        case BIG_INTEGER:
        case INTEGER:
          return new FieldLiteral(Integer.valueOf(column.getDefaultValue())).as(column.getName());
        case DECIMAL:
          return new FieldLiteral(Double.valueOf(column.getDefaultValue())).as(column.getName());
        default:
          throw new UnsupportedOperationException("Cannot determine the default value for data of type " + column.getType());
      }
    } else {
      switch (column.getType()) {
        case STRING:
          return new FieldLiteral("").as(column.getName());
        case BOOLEAN:
          return new FieldLiteral(false).as(column.getName());
        case DECIMAL:
        case INTEGER:
        case BIG_INTEGER:
          return new FieldLiteral(0).as(column.getName());
        default:
          throw new UnsupportedOperationException("Cannot determine the default value for data of type " + column.getType());
      }
    }
  }


  /**
   * Determines whether the column is a type that will always be defaulted to
   * 'null'. i.e. dates, BLOBs and CLOBs.
   *
   * @param column the column to check for.
   * @return true if the column should default to null.
   */
  private boolean isNullDefaultType(Column column) {
    return column.getType() == DataType.DATE || column.getType() == DataType.BLOB || column.getType() == DataType.CLOB;
  }
}
