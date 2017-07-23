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

package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.util.DeepCopyTransformations.noTransformation;
import static org.alfasoftware.morf.util.DeepCopyTransformations.transformIterable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

/**
 * Class which encapsulates the generation of an INSERT SQL statement.
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 *   new InsertStatement()
 *        |----&gt; .into([table])                       = INSERT INTO [table]
 *                |----&gt; .fields([field], ...)        = INSERT INTO [table] ([field], ...)
 *                |----&gt; .from([selectStatement])     = INSERT INTO [table] (SELECT * FROM ...)
 *                |----&gt; .from([table])               = INSERT INTO [table] (SELECT * FROM [table])
 * </pre></blockquote>
 *
 * <p>To insert a specific set of values (e.g. a new transaction code row) do the following:</p>
 * <blockquote><pre>
 *   new InsertStatement().into(new TableReference("TableName")).values(
 *     new FieldLiteral("SomeValue").as("TheColumnName"),
 *     new FieldLiteral(11).as("AnIntegerColumnName")
 *     ... etc
 *   );
 * </pre></blockquote>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class InsertStatement implements Statement, DeepCopyableWithTransformation<InsertStatement, Builder<InsertStatement>>, Driver{

  /**
   * The fields to select from the table
   */
  private final List<AliasedField>  fields        = new ArrayList<>();

  /**
   * The primary table to select from
   */
  private TableReference            table;

  /**
   * The select statement to source the data from
   */
  private SelectStatement           selectStatement;

  /**
   * The table to select from. This is a short-hand method.
   */
  private TableReference            fromTable;

  /**
   * List of literal field values to insert to when not selecting data from another table.
   */
  private final List<AliasedField>  values = new ArrayList<>();

  /**
   * The default values to use when the field values are not known
   */
  private final Map<String, AliasedField> fieldDefaults = new HashMap<>();


  /**
   * Constructs an Insert Statement.
   */
  public InsertStatement() {
    super();
  }


  /**
   * Constructor to create a deep copy.
   *
   * @param sourceStatement {@link InsertStatement} to create a deep copy from.
   */
  private InsertStatement(InsertStatement sourceStatement, DeepCopyTransformation transformation) {
      super();

    this.fields.addAll(transformIterable(sourceStatement.fields,transformation));
    this.values.addAll(transformIterable(sourceStatement.values,transformation));

    for (String fieldKey : sourceStatement.fieldDefaults.keySet()) {
      AliasedField currentDefault = sourceStatement.fieldDefaults.get(fieldKey);
      this.fieldDefaults.put(fieldKey, transformation.deepCopy(currentDefault));
    }

    this.table = transformation.deepCopy(sourceStatement.table);
    this.fromTable = transformation.deepCopy(sourceStatement.fromTable);
    this.selectStatement = transformation.deepCopy(sourceStatement.selectStatement);
  }


  /**
   * Inserts into a specific table.
   *
   * <blockquote><pre>
   *    new InsertStatement().into(new TableReference("agreement"));</pre></blockquote>
   *
   * @param intoTable the table to insert into
   * @return the updated InsertStatement (this will not be a new object)
   */
  public InsertStatement into(TableReference intoTable) {
    this.table = intoTable;
    return this;
  }


  /**
   * Specifies the fields to insert into the table.
   *
   * <p>
   * NOTE: This method should not be used in conjunction with {@link #values}.
   * </p>
   *
   * @param destinationFields the fields to insert into the database table
   * @return the updated InsertStatement (this will not be a new object)
   */
  public InsertStatement fields(AliasedFieldBuilder... destinationFields) {
   return fields(Lists.newArrayList(destinationFields));
  }


  /**
   * Specifies the fields to insert into the table.
   *
   * <p>
   * NOTE: This method should not be used in conjunction with {@link #values}.
   * </p>
   *
   * @param destinationFields the fields to insert into the database table
   * @return the updated InsertStatement (this will not be a new object)
   */
  public InsertStatement fields(Iterable<? extends AliasedFieldBuilder> destinationFields) {
    if (fromTable != null) {
      throw new UnsupportedOperationException("Cannot specify both a source table and a list of fields");
    }

    this.fields.addAll(Builder.Helper.<AliasedField>buildAll(destinationFields));
    return this;
  }


  /**
   * Specifies the select statement to use as a source of the data
   *
   * @param statement the source statement
   * @return the updated InsertStatement (this will not be a new object)
   */
  public InsertStatement from(SelectStatement statement) {
    if (fromTable != null) {
      throw new UnsupportedOperationException("Cannot specify both a source SelectStatement and a source table");
    }

    if (!values.isEmpty()) {
      throw new UnsupportedOperationException("Cannot specify both a source SelectStatement and a set of literal field values.");
    }

    this.selectStatement = statement;
    return this;
  }


  /**
   * Specifies the table to source the data from
   *
   * @param sourceTable the table to source the data from
   * @return the updated InsertStatement (this will not be a new object)
   *
   */
  public InsertStatement from(TableReference sourceTable) {
    if (selectStatement != null) {
      throw new UnsupportedOperationException("Cannot specify both a source table and a source SelectStatement");
    }

    if (!fields.isEmpty()) {
      throw new UnsupportedOperationException("Cannot specify both a source table and a list of fields");
    }

    if (!values.isEmpty()) {
      throw new UnsupportedOperationException("Cannot specify both a source table and a set of literal field values.");
    }

    this.fromTable = sourceTable;
    return this;
  }


  /**
   * Specifies the literal field values to insert.
   *
   * <p>
   * Each field must have an alias which specifies the column to insert into.
   * </p>
   *
   * @see AliasedField#as(String)
   * @param fieldValues Literal field values to insert.
   * @return the updated InsertStatement (this will not be a new object)
   */
  public InsertStatement values(AliasedFieldBuilder... fieldValues) {
    if (fromTable != null) {
      throw new UnsupportedOperationException("Cannot specify both a literal set of field values and a from table.");
    }

    if (selectStatement != null) {
      throw new UnsupportedOperationException("Cannot specify both a literal set of field values and a sub-select statement.");
    }

    this.values.addAll(Builder.Helper.buildAll(Lists.newArrayList(fieldValues)));
    return this;
  }


  /**
   * Specifies the defaults to use when inserting new fields
   *
   * @param defaultValues the list of {@linkplain FieldReference}s to use as defaults
   * @return the updated InsertStatement (this will not be a new object)
   */
  public InsertStatement withDefaults(AliasedFieldBuilder... defaultValues) {
    for(AliasedField currentValue : Builder.Helper.buildAll(Lists.newArrayList(defaultValues))) {
      if (StringUtils.isBlank(currentValue.getAlias())) {
        throw new IllegalArgumentException("Cannot specify a blank alias for a field default");
      }

      fieldDefaults.put(currentValue.getAlias(), currentValue);
    }
    return this;
  }


  /**
   * Identifies whether this insert is a parameterised insert
   * with no source table or select.
   *
   * @return true if this is a parameterised insert statement, false otherwise
   */
  public boolean isParameterisedInsert() {
    return fromTable == null && selectStatement == null && values.isEmpty();
  }


  /**
   * Identifies whether this insert will use specified
   * {@link #values(AliasedFieldBuilder...)} instead of a source
   * table or select.
   *
   * @return true if the insert is using specified actual values to insert into the columns.
   */
  public boolean isSpecificValuesInsert() {
    return fromTable == null && selectStatement == null && !values.isEmpty();
  }


  /**
   * Gets the list of fields to insert
   *
   * @return the fields being inserted into
   */
  public List<AliasedField> getFields() {
    return fields;
  }


  /**
   * Gets the table being inserted into
   *
   * @return the table being inserted into
   */
  public TableReference getTable() {
    return table;
  }


  /**
   * Gets the select statement which will generate the data for the insert.
   *
   * @return the select statement to use, or null if none is specified.
   */
  public SelectStatement getSelectStatement() {
    return selectStatement;
  }


  /**
   * Gets the table to select from. This is a short-hand for "SELECT * FROM [Table]".
   *
   * @return the table to select from.
   */
  public TableReference getFromTable() {
    return fromTable;
  }


  /**
   * Gets the field defaults that should be used when inserting new fields.
   *
   * @return a map of field names to field default values to use during this insert.
   */
  public Map<String, AliasedField> getFieldDefaults() {
    return fieldDefaults;
  }


  /**
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  public InsertStatement deepCopy() {
    return new InsertStatement(this,noTransformation());
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<InsertStatement> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new InsertStatement(this,transformer));
  }


  /**
   * @return List of literal field values to insert to when not selecting data from another table.
   */
  public List<AliasedField> getValues() {
    return values;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("SQL INSERT INTO [" + table + "]");
    if (!fields.isEmpty()) result.append(" FIELDS ").append(fields);
    if (!values.isEmpty()) result.append(" VALUES ").append(values);
    if (selectStatement != null) result.append(" FROM SELECT [").append(selectStatement).append("]");
    if (fromTable != null) result.append(" FROM [" + fromTable + "]");
    return result.toString();
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getTable())
      .dispatch(getSelectStatement())
      .dispatch(getFields())
      .dispatch(getValues())
      .dispatch(getFieldDefaults().values());
  }
}
