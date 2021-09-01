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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.alfasoftware.morf.util.ShallowCopyable;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Class which encapsulates the generation of an INSERT SQL statement.
 *
 * <p>The class structure imitates the end SQL and is constructed using a builder, as follows:</p>
 *
 * <blockquote><pre>
 *  InsertStatement.insert()
 *    .into([table])               = INSERT INTO [table]
 *    .fields([field], ...)        = INSERT INTO [table] ([field], ...)
 *    .from([selectStatement])     = INSERT INTO [table] (SELECT * FROM ...)
 *    .from([table])               = INSERT INTO [table] (SELECT * FROM [table])
 *    .build()
 * </pre></blockquote>
 *
 * <p>To insert a specific set of values, do the following:</p>
 *
 * <blockquote><pre>
 *   InsertStatement.insert()
 *     .into(tableRef("TableName"))
 *     .values(
 *       literal("SomeValue").as("TheColumnName"),
 *       literal(11).as("AnIntegerColumnName")
 *       ... etc
 *     )
 *     .build();
 * </pre></blockquote>
 *
 * <p>It is also possible to create instances directly using the constructors or the factory
 * methods on {@link SqlUtils}.  Both are discouraged and will be deprecated in the future.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class InsertStatement implements Statement,
                                        DeepCopyableWithTransformation<InsertStatement, InsertStatementBuilder>,
                                        ShallowCopyable<InsertStatement, InsertStatementBuilder>,
                                        Driver {

  /**
   * The fields to select from the table
   */
  private final List<AliasedField>  fields;

  /**
   * Lists the declared hints in the order they were declared.
   */
  private final List<Hint> hints;

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
  private final List<AliasedField>  values;

  /**
   * The default values to use when the field values are not known
   */
  private final Map<String, AliasedField> fieldDefaults;


  /**
   * Creates a new insert statement. See class-level documentation for usage instructions.
   *
   * @return A builder.
   */
  public static InsertStatementBuilder insert() {
    return new InsertStatementBuilder();
  }


  /**
   * Constructs an Insert Statement.
   *
   * <p>Usage is discouraged; this method will be deprecated at some point. Use
   * {@link #insert()} for preference.</p>
   */
  public InsertStatement() {
    super();
    if (AliasedField.immutableDslEnabled()) {
      this.fields = ImmutableList.of();
      this.hints = ImmutableList.of();
      this.values = ImmutableList.of();
      this.fieldDefaults = ImmutableMap.of();
    } else {
      this.fields = new ArrayList<>();
      this.hints = new ArrayList<>();
      this.values = new ArrayList<>();
      this.fieldDefaults = new HashMap<>();
    }
  }


  /**
   * Constructor for use by the builder.
   *
   * @param builder The builder.
   */
  InsertStatement(InsertStatementBuilder builder) {
    super();
    this.table = builder.getTable();
    this.fromTable = builder.getFromTable();
    this.selectStatement = builder.getSelectStatement();
    if (AliasedField.immutableDslEnabled()) {
      this.fields = ImmutableList.copyOf(builder.getFields());
      this.hints = ImmutableList.copyOf(builder.getHints());
      this.values = ImmutableList.copyOf(builder.getValues());
      this.fieldDefaults = ImmutableMap.copyOf(builder.getFieldDefaults());
    } else {
      this.fields = new ArrayList<>();
      this.hints = new ArrayList<>();
      this.values = new ArrayList<>();
      this.fieldDefaults = new HashMap<>();
      this.fields.addAll(builder.getFields());
      this.values.addAll(builder.getValues());
      this.fieldDefaults.putAll(builder.getFieldDefaults());
      this.hints.addAll(builder.getHints());
    }
  }


  /**
   * Inserts into a specific table.
   *
   * <blockquote><pre>insert().into(tableRef("agreement"));</pre></blockquote>
   *
   * @param intoTable the table to insert into.
   * @return a statement with the changes applied.
   */
  public InsertStatement into(TableReference intoTable) {
    return copyOnWriteOrMutate(
        b -> b.into(intoTable),
        () -> this.table = intoTable
    );
  }


  /**
   * Either uses {@link #shallowCopy()} and mutates the result, returning it,
   * or mutates the statement directly, depending on
   * {@link AliasedField#immutableDslEnabled()}.
   *
   * TODO for removal along with mutable behaviour.
   *
   * @param transform A transform which modifies the shallow copy builder.
   * @param mutator Code which applies the local changes instead.
   * @return The result (which may be {@code this}).
   */
  private InsertStatement copyOnWriteOrMutate(Function<InsertStatementBuilder, InsertStatementBuilder> transform, Runnable mutator) {
    if (AliasedField.immutableDslEnabled()) {
      return transform.apply(shallowCopy()).build();
    } else {
      mutator.run();
      return this;
    }
  }


  /**
   * Specifies the fields to insert into the table.
   *
   * <p>
   * NOTE: This method should not be used in conjunction with {@link #values}.
   * </p>
   *
   * @param destinationFields the fields to insert into the database table
   * @return a statement with the changes applied.
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
   * @return a statement with the changes applied.
   */
  public InsertStatement fields(Iterable<? extends AliasedFieldBuilder> destinationFields) {
    return copyOnWriteOrMutate(
        b -> b.fields(destinationFields),
        () -> {
          if (fromTable != null) {
            throw new UnsupportedOperationException("Cannot specify both a source table and a list of fields");
          }
          this.fields.addAll(Builder.Helper.<AliasedField>buildAll(destinationFields));
        }
    );
  }


  /**
   * Specifies the select statement to use as a source of the data
   *
   * @param statement the source statement
   * @return a statement with the changes applied.
   */
  public InsertStatement from(SelectStatement statement) {
    return copyOnWriteOrMutate(
        b -> b.from(statement),
        () -> {
          if (fromTable != null) {
            throw new UnsupportedOperationException("Cannot specify both a source SelectStatement and a source table");
          }
          if (!values.isEmpty()) {
            throw new UnsupportedOperationException("Cannot specify both a source SelectStatement and a set of literal field values.");
          }
          this.selectStatement = statement;
        }
    );
  }


  /**
   * Specifies the table to source the data from
   *
   * @param sourceTable the table to source the data from
   * @return a statement with the changes applied.
   *
   */
  public InsertStatement from(TableReference sourceTable) {
    return copyOnWriteOrMutate(
        b -> b.from(sourceTable),
        () -> {
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
        }
    );
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
   * @return a statement with the changes applied.
   */
  public InsertStatement values(AliasedFieldBuilder... fieldValues) {
    return copyOnWriteOrMutate(
        b -> b.values(fieldValues),
        () -> {
          if (fromTable != null) {
            throw new UnsupportedOperationException("Cannot specify both a literal set of field values and a from table.");
          }
          if (selectStatement != null) {
            throw new UnsupportedOperationException("Cannot specify both a literal set of field values and a sub-select statement.");
          }
          this.values.addAll(Builder.Helper.buildAll(Lists.newArrayList(fieldValues)));
        }
    );
  }


  /**
   * If supported by the dialect, hints to the database that an {@code APPEND} query hint should be used in the insert statement.
   *
   * <p>In general, as with all query plan modification, <strong>do not use this unless you know
   * exactly what you are doing</strong>.</p>
   *
   * <p>These directives are applied in the SQL in the order they are called on {@link InsertStatement}.  This usually
   * affects their precedence or relative importance, depending on the platform.</p>
   *
   * @return a new insert statement with the change applied.
   */
  public InsertStatement useDirectPath() {
    return copyOnWriteOrMutate(
        InsertStatementBuilder::useDirectPath,
        () -> this.hints.add(DirectPathQueryHint.getInstance())
    );
  }


  /**
   * Specifies the defaults to use when inserting new fields
   *
   * @param defaultValues the list of values to use as defaults
   * @return a statement with the changes applied.
   */
  public InsertStatement withDefaults(AliasedFieldBuilder... defaultValues) {
    return copyOnWriteOrMutate(
        b -> b.withDefaults(defaultValues),
        () -> {
          for (AliasedField currentValue : Builder.Helper.buildAll(Lists.newArrayList(defaultValues))) {
            if (StringUtils.isBlank(currentValue.getAlias())) {
              throw new IllegalArgumentException("Cannot specify a blank alias for a field default");
            }
            fieldDefaults.put(currentValue.getAlias(), currentValue);
          }
        }
    );
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
   * @return all hints in the order they were declared.
   */
  public List<Hint> getHints() {
    return hints;
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
   * Performs a shallow copy to a builder, allowing a duplicate
   * to be created and modified.
   *
   * @return A builder, initialised as a duplicate of this statement.
   */
  @Override
  public InsertStatementBuilder shallowCopy() {
    return new InsertStatementBuilder(this);
  }


  /**
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  public InsertStatement deepCopy() {
    return new InsertStatementBuilder(this, noTransformation()).build();
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public InsertStatementBuilder deepCopy(DeepCopyTransformation transformer) {
    return new InsertStatementBuilder(this, transformer);
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
    if (!hints.isEmpty()) result.append(" HINTS ").append(hints);
    if (!fields.isEmpty()) result.append(" FIELDS ").append(fields);
    if (!values.isEmpty()) result.append(" VALUES ").append(values);
    if (selectStatement != null) result.append(" FROM SELECT [").append(selectStatement).append("]");
    if (fromTable != null) result.append(" FROM [" + fromTable + "]");
    if (!fieldDefaults.isEmpty()) result.append(" WITH DEFAULTS ").append(fieldDefaults.values());
    return result.toString();
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (fieldDefaults == null ? 0 : fieldDefaults.hashCode());
    result = prime * result + (hints == null ? 0 : hints.hashCode());
    result = prime * result + (fields == null ? 0 : fields.hashCode());
    result = prime * result + (fromTable == null ? 0 : fromTable.hashCode());
    result = prime * result + (selectStatement == null ? 0 : selectStatement.hashCode());
    result = prime * result + (table == null ? 0 : table.hashCode());
    result = prime * result + (values == null ? 0 : values.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    InsertStatement other = (InsertStatement) obj;
    if (fieldDefaults == null) {
      if (other.fieldDefaults != null)
        return false;
    } else if (!fieldDefaults.equals(other.fieldDefaults))
      return false;
    if (fields == null) {
      if (other.fields != null)
        return false;
    } else if (!fields.equals(other.fields))
      return false;
    if (hints == null) {
      if (other.hints != null)
        return false;
    } else if (!hints.equals(other.hints))
      return false;
    if (fromTable == null) {
      if (other.fromTable != null)
        return false;
    } else if (!fromTable.equals(other.fromTable))
      return false;
    if (selectStatement == null) {
      if (other.selectStatement != null)
        return false;
    } else if (!selectStatement.equals(other.selectStatement))
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    if (values == null) {
      if (other.values != null)
        return false;
    } else if (!values.equals(other.values))
      return false;
    return true;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(table)
      .dispatch(fromTable)
      .dispatch(selectStatement)
      .dispatch(fields)
      .dispatch(values)
      .dispatch(fieldDefaults.values());
  }
}
