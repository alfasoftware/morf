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

import java.util.Optional;

import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;

/**
 * Builder for {@link DeleteStatement}. Create a new statement Using
 * {@link DeleteStatement#delete(TableReference)}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class DeleteStatementBuilder implements Builder<DeleteStatement> {

  /**
   * The table to update
   */
  private final TableReference table;


  /**
   * The selection criteria for selecting from the database.
   */
  private Criterion whereCriterion;


  /**
   * The limit for the number of rows to be deleted.
   */
  private Optional<Integer> limit = Optional.empty();


  /**
   * Constructor to create a shallow copy.
   *
   * @param sourceStatement {@link DeleteStatement} to create a shallow copy from.
   */
  DeleteStatementBuilder(DeleteStatement sourceStatement) {
    this.table = sourceStatement.getTable();
    this.whereCriterion = sourceStatement.getWhereCriterion();
    this.limit = sourceStatement.getLimit();
  }


  /**
   * Constructor to create a deep copy.
   *
   * @param sourceStatement {@link DeleteStatement} to create a deep copy from.
   * @param transformer A transformation that can intercept a deep copy operation.
   */
  DeleteStatementBuilder(DeleteStatement sourceStatement, DeepCopyTransformation transformer) {
    this.table = transformer.deepCopy(sourceStatement.getTable());
    this.whereCriterion = transformer.deepCopy(sourceStatement.getWhereCriterion());
    this.limit = sourceStatement.getLimit();
  }


  /**
   * Constructs a Delete Statement.
   *
   * @param table the database table to delete from.
   */
  DeleteStatementBuilder(TableReference table) {
    super();
    this.table = table;
  }


  /**
   * Gets the table being deleted from.
   *
   * @return the table being inserted into
   */
  TableReference getTable() {
    return table;
  }


  /**
   * Specifies the where criteria
   *
   * <blockquote><pre>DeleteStatement.delete([table])
   *    .where([criteria])
   *    .build();</pre></blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return this, for method chaining.
   */
  public DeleteStatementBuilder where(Criterion criterion) {
    if (criterion == null)
      throw new IllegalArgumentException("Criterion was null in where clause");

    whereCriterion = criterion;

    return this;
  }


  /**
   * Gets the where criteria.
   *
   * @return the where criteria
   */
  Criterion getWhereCriterion() {
    return whereCriterion;
  }


  /**
   * Specifies the limit for the delete statement.
   *
   * <blockquote><pre>DeleteStatement.delete([table])
   *    .where([criteria])
   *    .limit(1000)
   *    .build();</pre></blockquote>
   *
   * @param limit the limit on the number of deleted records.
   * @return this, for method chaining.
   */
  public DeleteStatementBuilder limit(int limit) {
    this.limit = Optional.of(limit);
    return this;
  }


  /**
   * Gets the limit.
   *
   * @return the limit on the number of deleted records.
   */
  public Optional<Integer> getLimit() {
    return limit;
  }


  @Override
  public DeleteStatement build() {
    return new DeleteStatement(this);
  }
}