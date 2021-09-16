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
import java.util.List;

import org.alfasoftware.morf.sql.UnionSetOperator.UnionStrategy;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;

import com.google.common.collect.Lists;

/**
 * Builder for {@link SelectStatement}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class SelectStatementBuilder extends AbstractSelectStatementBuilder<SelectStatement, SelectStatementBuilder> {

  boolean distinct;
  Criterion having;
  boolean forUpdate;
  final List<AliasedField> groupBys = new ArrayList<>();
  final List<SetOperator> setOperators = new ArrayList<>();
  final List<Hint> hints = new ArrayList<>();


  SelectStatementBuilder() {
    super();
  }


  /**
   * Shallow copy constructor.
   *
   * @param copyOf The statement to copy.
   */
  SelectStatementBuilder(SelectStatement copyOf) {
    super(copyOf);
    this.distinct = copyOf.isDistinct();
    this.having = copyOf.getHaving();
    this.forUpdate = copyOf.isForUpdate();
    this.groupBys.addAll(copyOf.getGroupBys());
    this.setOperators.addAll(copyOf.getSetOperators());
    this.hints.addAll(copyOf.getHints());
  }


  /**
   * Deep copy constructor.
   *
   * @param copyOf The statement to copy.
   * @param transformation The transformation to apply.
   */
  SelectStatementBuilder(SelectStatement copyOf, DeepCopyTransformation transformation) {
    super(copyOf, transformation);
    this.distinct = copyOf.isDistinct();
    this.having = transformation.deepCopy(copyOf.getHaving());
    this.forUpdate = copyOf.isForUpdate();;
    this.groupBys.addAll(DeepCopyTransformations.transformIterable(copyOf.getGroupBys(), transformation));
    this.setOperators.addAll(DeepCopyTransformations.transformIterable(copyOf.getSetOperators(),transformation));
    this.hints.addAll(copyOf.getHints()); // FIXME these should get transformed - UseIndex in particular
  }


  /**
   * Tells the database to pessimistically lock the tables.
   *
   * @return this, for method chaining.
   */
  public SelectStatementBuilder forUpdate() {
    this.forUpdate = true;
    return this;
  }


  /**
   * Disables pessimistic locking.
   *
   * @return this, for method chaining.
   */
  public SelectStatementBuilder notForUpdate() {
    this.forUpdate = false;
    return this;
  }


  /**
   * Use DISTINCT.
   *
   * @return this, for method chaining.
   */
  public SelectStatementBuilder distinct() {
    this.distinct = true;
    return this;
  }


  /**
   * Disable DISTINCT.  Although this is the default, it can be used
   * to remove the DISTINCT behaviour on a copied statement.
   *`
   * @return this, for method chaining.
   */
  public SelectStatementBuilder notDistinct() {
    this.distinct = false;
    return this;
  }


  /**
   * Specifies that the records should be grouped by the specified fields.
   *
   * <blockquote><pre>
   * select()
   *   .from(tableRef("Foo"))
   *   .groupBy(field("age"));</pre></blockquote>
   *
   * @param field the first field to group by
   * @param otherFields the remaining fields to group by
   * @return this, for method chaining.
   */
  public SelectStatementBuilder groupBy(AliasedFieldBuilder field, AliasedFieldBuilder... otherFields) {
    if (field == null) {
      throw new IllegalArgumentException("Field was null in group by clause");
    }

    // Add the singleton
    groupBys.add(field.build());

    // Add the list
    groupBys.addAll(Builder.Helper.buildAll(Lists.newArrayList(otherFields)));

    return this;
  }


  /**
   * Specifies that the records should be grouped by the specified fields.
   *
   * <blockquote><pre>
   * select()
   *   .from(tableRef("Foo"))
   *   .groupBy(field("age"));</pre></blockquote>
   *
   * @param fields the fields to group by
   * @return this, for method chaining.
   */
  public SelectStatementBuilder groupBy(Iterable<? extends AliasedFieldBuilder> fields) {
    // Add the list
    groupBys.addAll(Builder.Helper.buildAll(Lists.newArrayList(fields)));

    return this;
  }


  /**
   * Filters the grouped records by some criteria.
   *
   * <blockquote><pre>
   * select()
   *   .from(tableRef("Foo"))
   *   .groupBy(field("age"))
   *   .having(min(field("age")).greaterThan(20));</pre></blockquote>
   *
   * @param criterion the criteria on which to filter the grouped records
   * @return this, for method chaining.
   */
  public SelectStatementBuilder having(Criterion criterion) {
    if (criterion == null) {
      throw new IllegalArgumentException("Criterion was null in having clause");
    }

    if (having != null) {
      throw new UnsupportedOperationException("Cannot specify more than one having clause per statement");
    }

    // Add the singleton
    having = criterion;

    return this;
  }


  /**
   * If supported by the dialect, requests that the database arranges the query plan such that the
   * first <code>rowCount</code> rows are returned as fast as possible, even if the statistics
   * indicate that this means the full result set will take longer to be returned.
   *
   * <p>Note that this is <em>not</em> to be confused with directives which limit the number of
   * rows to be returned.  It does not limit the number of rows, only optimises the query plan
   * to target returning that number as fast as possible.</p>
   *
   * <p>In general, as with all query plan modification, <strong>do not use this unless you know
   * exactly what you are doing</strong>.  This is designed to support the case where the
   * {@link SelectStatement} is actually a driver query, and for each record, other processing
   * is being performed.  Since the processing as a whole may involve not just the database
   * but also the application server cluster, secondary databases or external systems, we are
   * making the educated guess that by trying to get processing running in parallel across
   * all these systems as soon as possible, we will achieve a higher overall throughput than
   * having these other systems waiting around until the database has finished doing
   * preparatory work to try and optimise its own throughput.</p>
   *
   * <p>As for all query plan modification (see also {@link #useIndex(TableReference, String)}
   * and {@link #useImplicitJoinOrder()}): where supported on the target database, these directives
   * applied in the SQL in the order they are called on {@link SelectStatement}.  This usually
   * affects their precedence or relative importance, depending on the platform.</p>
   *
   * @param rowCount The number of rows for which to optimise the query plan.
   * @return this, for method chaining.
   */
  public SelectStatementBuilder optimiseForRowCount(int rowCount) {
    this.hints.add(new OptimiseForRowCount(rowCount));
    return this;
  }


  /**
   * If supported by the dialect, hints to the database that a particular index should be used
   * in the query, but places no obligation on the database to do so.
   *
   * <p>In general, as with all query plan modification, <strong>do not use this unless you know
   * exactly what you are doing</strong>.</p>
   *
   * <p>As for all query plan modification (see also {@link #optimiseForRowCount(int)}
   * and {@link #useImplicitJoinOrder()}): where supported on the target database, these directives
   * applied in the SQL in the order they are called on {@link SelectStatement}.  This usually
   * affects their precedence or relative importance, depending on the platform.</p>
   *
   * @param table The table whose index to use.
   * @param indexName The name of the index to use.
   * @return this, for method chaining.
   */
  public SelectStatementBuilder useIndex(TableReference table, String indexName) {
    this.hints.add(new UseIndex(table, indexName));
    return this;
  }


  /**
   * Request that this query is executed with a parallel execution plan. If the database implementation does not support, or is configured to disable parallel query execution, then this request will have no effect.
   *
   * <p>For queries that are likely to conduct a full table scan, a parallel execution plan may result in the results being delivered faster, although the exact effect depends on
   * the underlying database, the nature of the data and the nature of the query.</p>
   *
   * <p>Note that the executed use cases of this are rare. Caution is needed because if multiple requests are made by the application to run parallel queries, the resulting resource contention may result in worse performance - this is not intended for queries that are submitted in parallel by the application.</p>
   *
   * @return this, for method chaining.
   */
  public SelectStatementBuilder withParallelQueryPlan() {
    this.hints.add(new ParallelQueryHint());
    return this;
  }


  /**
   * Request that this query is executed with a parallel execution plan and with the given degree of parallelism. If the database implementation does not support, or is configured to disable parallel query execution, then this request will have no effect.
   *
   * <p>For queries that are likely to conduct a full table scan, a parallel execution plan may result in the results being delivered faster, although the exact effect depends on
   * the underlying database, the nature of the data and the nature of the query.</p>
   *
   * <p>Note that the executed use cases of this are rare. Caution is needed because if multiple requests are made by the application to run parallel queries, the resulting resource contention may result in worse performance - this is not intended for queries that are submitted in parallel by the application.</p>
   *
   * @return this, for method chaining.
   */
  public SelectStatementBuilder withParallelQueryPlan(int degreeOfParallelism) {
    this.hints.add(new ParallelQueryHint(degreeOfParallelism));
    return this;
  }


  /**
   * If supported by the dialect, hints to the database that joins should be applied in the order
   * they are written in the SQL statement.
   *
   * <p>This is supported to greater or lesser extends on different SQL dialects.  For instance,
   * MySQL has no means to force ordering on anything except inner joins, but we do our best. As
   * a result, this is not a panacea and may need to be combined with
   * {@link #useIndex(TableReference, String)} to achieve a consistent effect across
   * platforms.</p>
   *
   * <p>In general, as with all query plan modification, <strong>do not use this unless you know
   * exactly what you are doing</strong>.</p>
   *
   * <p>As for all query plan modification (see also {@link #optimiseForRowCount(int)}
   * and {@link #useIndex(TableReference, String)}): where supported on the target database, these directives
   * applied in the SQL in the order they are called on {@link SelectStatement}.  This usually
   * affects their precedence or relative importance, depending on the platform.</p>
   *
   * @return this, for method chaining.
   */
  public SelectStatementBuilder useImplicitJoinOrder() {
    this.hints.add(new UseImplicitJoinOrder());
    return this;
  }


  /**
   * Perform an UNION set operation with another {@code selectStatement},
   * eliminating any duplicate rows.
   *
   * <p>It is possible to have more than one union statement by chaining union calls:</p>
   * <blockquote><pre>
   * SelectStatement stmtA = select(...).from(...);
   * SelectStatement stmtB = select(...).from(...);
   * SelectStatement stmtC = select(...).from(...).union(stmtA).union(stmtB).orderBy(...);
   * </pre></blockquote>
   *
   * <p>If an union operation is performed then all
   * participating select statements require the same selected column list, i.e.
   * same naming and ordering. In addition, only the leftmost select statement
   * should have an order-by statement (see example above).</p>
   *
   * @param selectStatement the select statement to be united with the current select statement;
   * @return this, for method chaining.
   */
  public SelectStatementBuilder union(SelectStatement selectStatement) {
    setOperators.add(new UnionSetOperator(UnionStrategy.DISTINCT, this.build(), selectStatement));
    return this;
  }


  /**
   * Perform an UNION set operation with another {@code selectStatement},
   * keeping all duplicate rows.
   *
   * @param selectStatement the other select statement to be united with the current select statement;
   * @return this, for method chaining.
   * @see #union(SelectStatement)
   */
  public SelectStatementBuilder unionAll(SelectStatement selectStatement) {
    setOperators.add(new UnionSetOperator(UnionStrategy.ALL, this.build(), selectStatement));
    return this;
  }


  /**
   * Gets the grouped fields
   *
   * @return the group by fields
   */
  List<AliasedField> getGroupBys() {
    return groupBys;
  }


  /**
   * @return the list of set operators to be applied on this select statement.
   */
  List<SetOperator> getSetOperators() {
    return setOperators;
  }


  /**
   * @return all hints in the order they were declared.
   */
  List<Hint> getHints() {
    return hints;
  }


  /**
   * Builds the select statement.
   */
  @Override
  public SelectStatement build() {
    return new SelectStatement(this);
  }
}