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
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;

/**
 * Builder for {@link UpdateStatement}.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */

public class UpdateStatementBuilder implements Builder<UpdateStatement> {

  private final TableReference table;
  private final List<AliasedField> fields   = new ArrayList<>();
  private final List<Hint> hints = new ArrayList<>();
  private Criterion whereCriterion;


  /**
   * Constructor to create a shallow copy.
   *
   * @param sourceStatement {@link UpdateStatement} to create a deep copy from.
   */
  UpdateStatementBuilder(UpdateStatement sourceStatement) {
    super();
    this.fields.addAll(sourceStatement.getFields());
    this.hints.addAll(sourceStatement.getHints());
    this.table = sourceStatement.getTable();
    this.whereCriterion = sourceStatement.getWhereCriterion();
  }


  /**
   * Constructor to create a deep copy.
   *
   * @param sourceStatement {@link UpdateStatement} to create a deep copy from.
   */
  UpdateStatementBuilder(UpdateStatement sourceStatement, DeepCopyTransformation transformation) {
    super();
    this.fields.addAll(DeepCopyTransformations.transformIterable(sourceStatement.getFields(), transformation));
    this.table = transformation.deepCopy(sourceStatement.getTable());
    this.whereCriterion = transformation.deepCopy(sourceStatement.getWhereCriterion());
  }


  /**
   * Constructs an Update Statement.
   *
   * @param table the database table to update
   */
  UpdateStatementBuilder(TableReference table) {
    super();
    this.table = table;
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
   * Specifies the fields to set.
   *
   * @param destinationFields the fields to update in the database table
   * @return this, for method chaining.
   */
  public UpdateStatementBuilder set(AliasedFieldBuilder... destinationFields) {
    this.fields.addAll(Builder.Helper.buildAll(Arrays.asList(destinationFields)));
    return this;
  }


  /**
   * Specifies the where criteria
   *
   * <blockquote><pre>
   *    update([table])
   *      .set([fields])
   *      .where([criteria]);</pre></blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return this, for method chaining.
   */
  public UpdateStatementBuilder where(Criterion criterion) {
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
   * Gets the list of fields
   *
   * @return the fields
   */
  List<AliasedField> getFields() {
    return fields;
  }


  /**
   * @return all hints in the order they were declared.
   */
  public List<Hint> getHints() {
    return hints;
  }


  /**
   * Request that this statement is executed with a parallel execution plan for data manipulation language (DML). This request will have no effect unless the database implementation supports it and the feature is enabled.
   *
   * <p>For statement that will affect a high percentage or rows in the table, a parallel execution plan may reduce the execution time, although the exact effect depends on
   * the underlying database, the nature of the data and the nature of the query.</p>
   *
   * <p>Note that the use of parallel DML comes with restrictions, in particular, a table may not be accessed in the same transaction following a parallel DML execution. Please consult the Oracle manual section <em>Restrictions on Parallel DML</em> to check whether this hint is suitable.</p>
   *
   * @return this, for method chaining.
   */
  public UpdateStatementBuilder useParallelDml() {
    hints.add(new UseParallelDml());
    return this;
  }

  /**
   * Request that this statement is executed with a parallel execution plan for data manipulation language (DML). This request will have no effect unless the database implementation supports it and the feature is enabled.
   *
   * <p>For statement that will affect a high percentage or rows in the table, a parallel execution plan may reduce the execution time, although the exact effect depends on
   * the underlying database, the nature of the data and the nature of the query.</p>
   *
   * <p>Note that the use of parallel DML comes with restrictions, in particular, a table may not be accessed in the same transaction following a parallel DML execution. Please consult the Oracle manual section <em>Restrictions on Parallel DML</em> to check whether this hint is suitable.</p>
   *
   * @param degreeOfParallelism - the degree of parallelism to be used
   * @return this, for method chaining.
   */
  public UpdateStatementBuilder useParallelDml(int degreeOfParallelism) {
    hints.add(new UseParallelDml(degreeOfParallelism));
    return this;
  }


  @Override
  public UpdateStatement build() {
    return new UpdateStatement(this);
  }
}