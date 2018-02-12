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

/**
 * Builder for {@link MergeStatement}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class MergeStatementBuilder implements Builder<MergeStatement> {

  private final List<AliasedField>  tableUniqueKey = new ArrayList<>();
  private TableReference            table;
  private SelectStatement           selectStatement;


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


  @Override
  public MergeStatement build() {
    return new MergeStatement(this);
  }
}