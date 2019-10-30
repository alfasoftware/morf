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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;

import com.google.common.collect.ImmutableList;

/**
 * Builder for {@link MergeStatement}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class MergeStatementBuilder implements Builder<MergeStatement> {

  private final List<AliasedField>  tableUniqueKey = new ArrayList<>();
  private TableReference            table;
  private SelectStatement           selectStatement;
  private final List<AliasedField>  ifUpdating = new ArrayList<>();


  /**
   * Constructor to create a new statement.
   */
  MergeStatementBuilder() {
    super();
  }


  /**
   * Constructor to create a shallow copy.
   *
   * @param sourceStatement {@link MergeStatementBuilder} to create a shallow copy from.
   */
  MergeStatementBuilder(MergeStatement sourceStatement) {
    super();
    this.table = sourceStatement.getTable();
    this.tableUniqueKey.addAll(sourceStatement.getTableUniqueKey());
    this.selectStatement = sourceStatement.getSelectStatement();
  }


  /**
   * Constructor to create a deep copy.
   *
   * @param sourceStatement {@link MergeStatementBuilder} to create a deep copy from.
   */
  MergeStatementBuilder(MergeStatement sourceStatement, DeepCopyTransformation transformer) {
    super();
    this.table = transformer.deepCopy(sourceStatement.getTable());
    this.tableUniqueKey.addAll(DeepCopyTransformations.transformIterable(sourceStatement.getTableUniqueKey(), transformer));
    this.selectStatement = transformer.deepCopy(sourceStatement.getSelectStatement());
  }


  /**
   * Merges into a specific table.
   *
   * <blockquote><pre>
   *    merge().into(new TableReference("agreement"));</pre></blockquote>
   *
   * @param intoTable the table to merge into.
   * @return this, for method chaining.
   */
  public MergeStatementBuilder into(TableReference intoTable) {
    this.table = intoTable;
    return this;
  }


  /**
   * <p>
   * Specifies the fields which make up a unique index or primary key on the
   * target table.
   * </p>
   * <p>
   * These <em>must</em> fully match a unique index or primary key, otherwise
   * this statement will fail on MySQL.  Note also potential issues around having
   * two unique indexes or a primary key and unique index, as detailed
   * <a href="http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html">here</a>.
   * </p>
   *
   * @param keyFields the key fields.
   * @return this, for method chaining.
   */
  public MergeStatementBuilder tableUniqueKey(AliasedFieldBuilder... keyFields) {
    return tableUniqueKey(Arrays.asList(keyFields));
  }


  /**
   * <p>
   * Specifies the fields which make up a unique index or primary key on the
   * target table.
   * </p>
   * <p>
   * These <em>must</em> fully match a unique index or primary key, otherwise
   * this statement will fail on MySQL.  Note also potential issues around having
   * two unique indexes or a primary key and unique index, as detailed
   * <a href="http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html">here</a>.
   * </p>
   *
   * @param keyFields the key fields.
   * @return this, for method chaining.
   */
  public MergeStatementBuilder tableUniqueKey(List<? extends AliasedFieldBuilder> keyFields) {
    this.tableUniqueKey.addAll(Builder.Helper.buildAll(keyFields));
    return this;
  }


  /**
   * Specifies the select statement to use as a source of the data.
   *
   * @param statement the source statement.
   * @return this, for method chaining.
   */
  public MergeStatementBuilder from(SelectStatement statement) {
    if (statement.getOrderBys().size() != 0) {
      throw new IllegalArgumentException("ORDER BY is not permitted in the SELECT part of a merge statement (SQL Server limitation)");
    }
    this.selectStatement = statement;
    return this;
  }


  /**
   * Specifies the merge expressions to be used when updating existing records.
   *
   * <pre>
   * merge()
   *   .into(tableRef("BalanceAccumulator"))
   *   .tableUniqueKey(field("id"))
   *   .from(select()
   *           .fields(literal(17).as("id"))
   *           .fields(literal(123.45).as("lastAmount"))
   *           .fields(literal(123.45).as("balance"))
   *           .build())
   *   .ifUpdating((overrider, values) -&gt; overrider
   *     .set(values.input("lastAmount").as("lastAmount"))                                 // this is optional, since it is the default merge behaviour
   *     .set(values.input("balance").plus(values.existing("balance")).as("balance")))     // this explicitly defines how "balance" is accumulated via merges
   * </pre>
   *
   * @param mutator Lambda defining the merge expressions.
   * @return this, for method chaining.
   */
  public MergeStatementBuilder ifUpdating(UpdateValuesMutator mutator) {
    UpdateValuesOverrider overrider = new UpdateValuesOverrider();
    UpdateValues values = new UpdateValues(table);
    mutator.mutate(overrider, values);
    this.ifUpdating.addAll(Builder.Helper.buildAll(overrider.getUpdateExpressions()));
    return this;
  }


  /**
   * To be used with {@link MergeStatementBuilder#ifUpdating(UpdateValuesMutator)} as lambda
   * to define the merge expressions to be used when updating existing records.
   */
  @FunctionalInterface
  public interface UpdateValuesMutator {

    /**
     * Method to be implemented (ideally via a lambda) to specify the
     * merge expressions to be used when updating existing records.
     *
     * @param overrider to be used in builder pattern to specify the merge expressions.
     * @param values to be used to reference old and new field values within the merge expressions.
     */
    public void mutate(UpdateValuesOverrider overrider, UpdateValues values);
  }


  /**
   * To be used as a builder pattern in {@link MergeStatementBuilder#ifUpdating(UpdateValuesMutator)}
   * to specify the merge expressions to be used when updating existing records.
   */
  public static final class UpdateValuesOverrider {

    private final ImmutableList.Builder<AliasedField> expressions = ImmutableList.builder();

    /**
     * Adds a merge expression to be used when updating existing records.
     *
     * @param updateExpression the merge expressions, aliased as target field name.
     * @return this, for method chaining.
     */
    public UpdateValuesOverrider set(AliasedFieldBuilder updateExpression) {
      expressions.add(updateExpression.build());
      return this;
    }


    private Iterable<AliasedField> getUpdateExpressions() {
      return expressions.build();
    }
  }


  /**
   * @see MergeStatementBuilder#ifUpdating(UpdateValuesMutator)
   */
  public static final class UpdateValues {

    private final TableReference destinationTable;

    public UpdateValues(TableReference destinationTable) {
      this.destinationTable = destinationTable;
    }


    /**
     * For updating existing records, references the existing field value, i.e. the value before merge.
     * To reference the new value being merged, use {@link #input(String)}.
     *
     * @param name Name of the referenced field.
     * @return Reference to the existing field value.
     */
    public AliasedField existing(String name) {
      return destinationTable.field(name);
    }


    /**
     * For updating existing records, references the new field value being merged, i.e. the value provided by the select.
     * To reference the existing value being replaced, use {@link #existing(String)}.
     *
     * @param name Name of the referenced field.
     * @return Reference to the new field value being merged.
     */
    public AliasedField input(String name) {
      return new MergeStatement.InputField(name);
    }
  }


  /**
   * Gets the table to merge the data into.
   *
   * @return the table.
   */
  TableReference getTable() {
    return table;
  }


  /**
   * Gets the select statement that selects the values to merge
   * into the table.
   *
   * @return the select statement.
   */
  SelectStatement getSelectStatement() {
    return selectStatement;
  }


  /**
   * Gets a list of the fields used as the key upon which to match.
   *
   * @return the table unique key.
   */
  List<AliasedField> getTableUniqueKey() {
    return tableUniqueKey;
  }


  /**
   * Gets the list of expressions to be used when updating existing records.
   *
   * @return the expressions for updating
   */
  Iterable<AliasedField> getIfUpdating() {
    return ifUpdating;
  }


  @Override
  public MergeStatement build() {
    return new MergeStatement(this);
  }
}