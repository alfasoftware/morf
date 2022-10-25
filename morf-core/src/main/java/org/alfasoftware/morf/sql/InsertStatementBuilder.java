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

import static org.alfasoftware.morf.util.DeepCopyTransformations.transformIterable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Builder for {@link InsertStatement}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class InsertStatementBuilder implements Builder<InsertStatement> {

  private final List<AliasedField> fields = new ArrayList<>();
  private final List<Hint> hints = new ArrayList<>();
  private TableReference table;
  private SelectStatement selectStatement;
  private TableReference fromTable;
  private final List<AliasedField> values = new ArrayList<>();
  private final Map<String, AliasedField> fieldDefaults = new HashMap<>();


  /**
   * Base constructor.
   */
  InsertStatementBuilder() {
    super();
  }


  /**
   * Shallow copy constructor.
   *
   * @param copyOf The statement to copy.
   */
  InsertStatementBuilder(InsertStatement copyOf) {
    super();
    this.fields.addAll(copyOf.getFields());
    this.hints.addAll(copyOf.getHints());
    this.values.addAll(copyOf.getValues());
    this.fieldDefaults.putAll(copyOf.getFieldDefaults());
    this.table = copyOf.getTable();
    this.fromTable = copyOf.getFromTable();
    this.selectStatement = copyOf.getSelectStatement();
  }


  /**
   * Deep copy constructor.
   *
   * @param copyOf The statement to copy.
   * @param transformation The transformation to apply.
   */
  InsertStatementBuilder(InsertStatement copyOf, DeepCopyTransformation transformation) {
    super();
    this.fields.addAll(transformIterable(copyOf.getFields(), transformation));
    this.values.addAll(transformIterable(copyOf.getValues(),transformation));
    this.fieldDefaults.putAll(Maps.transformValues(copyOf.getFieldDefaults(), transformation::deepCopy));
    this.table = transformation.deepCopy(copyOf.getTable());
    this.fromTable = transformation.deepCopy(copyOf.getFromTable());
    this.selectStatement = transformation.deepCopy(copyOf.getSelectStatement());
    this.hints.addAll(copyOf.getHints());
  }


  /**
   * Inserts into a specific table.
   *
   * <blockquote><pre>
   *    new InsertStatement().into(new TableReference("agreement"));</pre></blockquote>
   *
   * @param intoTable the table to insert into.
   * @return this, for method chaining.
   */
  public InsertStatementBuilder into(TableReference intoTable) {
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
   * @return this, for method chaining.
   */
  public InsertStatementBuilder fields(AliasedFieldBuilder... destinationFields) {
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
   * @return this, for method chaining.
   */
  public InsertStatementBuilder fields(Iterable<? extends AliasedFieldBuilder> destinationFields) {
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
   * @return this, for method chaining.
   */
  public InsertStatementBuilder from(SelectStatement statement) {
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
   * @return this, for method chaining.
   */
  public InsertStatementBuilder from(TableReference sourceTable) {
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
   * @return this, for method chaining.
   */
  public InsertStatementBuilder values(AliasedFieldBuilder... fieldValues) {
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
   * Specifies the defaults to use when inserting new fields.
   *
   * @param defaultValues the list of values to use as defaults
   * @return this, for method chaining.
   */
  public InsertStatementBuilder withDefaults(AliasedFieldBuilder... defaultValues) {
    return withDefaults(Arrays.asList(defaultValues));
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
   * @return this, for method chaining.
   */
  public InsertStatementBuilder avoidDirectPath() {
    getHints().add(NoDirectPathQueryHint.INSTANCE);
    return this;
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
   * @return this, for method chaining.
   */
  public InsertStatementBuilder useDirectPath() {
    getHints().add(DirectPathQueryHint.INSTANCE);
    return this;
  }

  public InsertStatementBuilder useParallelDml() {
    getHints().add(new UseParallelDml());
    return this;
  }

  public InsertStatementBuilder useParallelDml(int degreeOfParallelism) {
    getHints().add(new UseParallelDml(degreeOfParallelism));
    return this;
  }

  /**
   * Specifies the defaults to use when inserting new fields.
   *
   * @param defaultValues the list of values to use as defaults
   * @return this, for method chaining.
   */
  public InsertStatementBuilder withDefaults(List<AliasedFieldBuilder> defaultValues) {
    for(AliasedField currentValue : Builder.Helper.buildAll(defaultValues)) {
      if (StringUtils.isBlank(currentValue.getAlias())) {
        throw new IllegalArgumentException("Cannot specify a blank alias for a field default");
      }
      fieldDefaults.put(currentValue.getAlias(), currentValue);
    }
    return this;
  }


  /**
   * Gets the list of fields to insert
   *
   * @return the fields being inserted into
   */
  List<AliasedField> getFields() {
    return fields;
  }


  /**
   * @return all hints in the order they were declared.
   */
  List<Hint> getHints() {
    return hints;
  }


  /**
   * Gets the table being inserted into
   *
   * @return the table being inserted into
   */
  TableReference getTable() {
    return table;
  }


  /**
   * Gets the select statement which will generate the data for the insert.
   *
   * @return the select statement to use, or null if none is specified.
   */
  SelectStatement getSelectStatement() {
    return selectStatement;
  }


  /**
   * Gets the table to select from. This is a short-hand for "SELECT * FROM [Table]".
   *
   * @return the table to select from.
   */
  TableReference getFromTable() {
    return fromTable;
  }


  /**
   * Gets the field defaults that should be used when inserting new fields.
   *
   * @return a map of field names to field default values to use during this insert.
   */
  Map<String, AliasedField> getFieldDefaults() {
    return fieldDefaults;
  }


  /**
   * @return List of literal field values to insert to when not selecting data from another table.
   */
  List<AliasedField> getValues() {
    return values;
  }


  /**
   * @see org.alfasoftware.morf.util.Builder#build()
   */
  @Override
  public InsertStatement build() {
    return new InsertStatement(this);
  }
}