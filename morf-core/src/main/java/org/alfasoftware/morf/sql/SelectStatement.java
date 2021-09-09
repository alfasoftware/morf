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
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.UnionSetOperator.UnionStrategy;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.alfasoftware.morf.util.ShallowCopyable;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Class which encapsulates the generation of an SELECT SQL statement.
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 *  SelectStatement.select([field], ...)
 *    .from([table])                       = SELECT [fields] FROM [table]
 *    .innerJoin([table], [criterion])     = SELECT [fields] FROM [table] INNER JOIN [table] ON [criterion]
 *    .leftOuterJoin(...)
 *    .where([criterion])                  = SELECT [fields] FROM [table] WHERE [criterion]
 *    .orderBy([fields])                   = SELECT [fields] FROM [table] ORDER BY [fields]
 *    .groupBy([fields])                   = SELECT [fields] FROM [table] GROUP BY [fields]
 *      .having([criterion])               = SELECT [fields] FROM [table] GROUP BY [fields] HAVING [criterion]
 *    .union([SelectStatement])            = SELECT [fields] FROM [table] UNION [SelectStatement]
 *    .unionAll([SelectStatement])         = SELECT [fields] FROM [table] UNION ALL [SelectStatement]
 *    .build()</pre></blockquote>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class SelectStatement extends AbstractSelectStatement<SelectStatement>
                          implements DeepCopyableWithTransformation<SelectStatement, SelectStatementBuilder>,
                                     ShallowCopyable<SelectStatement, SelectStatementBuilder>,
                                     Driver {

  /**
   * Indicates whether the select statement is required to return a distinct set of results
   */
  private final boolean distinct;

  /**
   * The fields to group together.
   */
  private final List<AliasedField> groupBys;

  /**
   * The criteria to use for the group selection
   *
   * TODO make final
   */
  private Criterion          having;

  /**
   * The set operators to merge with the result set.
   */
  private final List<SetOperator> setOperators;

  /**
   * WITH LOCK
   *
   * TODO make final
   */
  private boolean forUpdate;

  /**
   * Lists the declared hints in the order they were declared.
   */
  private final List<Hint> hints;

  private int hashCode;


  /**
   * Constructs a Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all
   * fields (i.e. {@code SELECT * FROM x}).
   *
   * @param fields an array of fields that should be selected
   * @return Builder.
   */
  public static SelectStatementBuilder select(AliasedFieldBuilder... fields) {
    return new SelectStatementBuilder().fields(fields);
  }


  /**
   * Constructs a Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all
   * fields (i.e. {@code SELECT * FROM x}).
   *
   * @param fields a list of fields that should be selected
   * @return Builder.
   */
  public static SelectStatementBuilder select(Iterable<AliasedFieldBuilder> fields) {
    return new SelectStatementBuilder().fields(fields);
  }


  /**
   * Builder constructor.
   *
   * @param builder The builder.
   */
  SelectStatement(SelectStatementBuilder builder) {
    super(builder);
    this.distinct = builder.distinct;
    this.having = builder.having;
    this.forUpdate = builder.forUpdate;
    if (AliasedField.immutableDslEnabled()) {
      this.groupBys = ImmutableList.copyOf(builder.groupBys);
      this.setOperators = ImmutableList.copyOf(builder.setOperators);
      this.hints = ImmutableList.copyOf(builder.hints);
    } else {
      this.groupBys = new ArrayList<>(builder.groupBys);
      this.setOperators = new ArrayList<>(builder.setOperators);
      this.hints = new ArrayList<>(builder.hints);
    }
  }


  /**
   * Constructs a Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all fields (i.e. "SELECT * FROM x")
   *
   * <p>Usage is discouraged; this constructor will be deprecated at some point. Use
   * {@link #select(AliasedFieldBuilder...)} for preference.</p>
   *
   * @param fields an array of fields that should be selected
   */
  public SelectStatement(AliasedFieldBuilder... fields) {
    this(fields, false);
  }


  /**
   * Constructs a Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all fields (i.e. "SELECT * FROM x")
   *
   * <p>Usage is discouraged; this method will be deprecated at some point. Use
   * {@link #select(AliasedFieldBuilder...)} for preference. For
   * example:</p>
   *
   * <pre>SelectStatement.select().fields(myFields).from(foo).build();</pre>
   *
   * @param fields an array of fields that should be selected
   */
  public SelectStatement(Iterable<? extends AliasedFieldBuilder> fields) {
    this(fields, false);
  }


  /**
   * Constructs a distinct Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all fields (i.e. "SELECT DISTINCT * FROM x")
   *
   * <p>Usage is discouraged; this method will be deprecated at some point. Use
   * {@link #select(AliasedFieldBuilder...)} for preference. For
   * example:</p>
   *
   * <pre>SelectStatement.select(myFields).distinct().from(foo).build();</pre>
   *
   * @param fields The fields in the select clause
   * @param isDistinct Determines whether the DISTINCT clause should be added or not
   */
  public SelectStatement(AliasedFieldBuilder[] fields, boolean isDistinct) {
    this(Arrays.asList(fields), isDistinct);
  }


  /**
   * Constructs a distinct Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all fields (i.e. "SELECT DISTINCT * FROM x")
   *
   * <p>Usage is discouraged; this method will be deprecated at some point. Use
   * {@link #select(AliasedFieldBuilder...)} for preference. For
   * example:</p>
   *
   * <pre>SelectStatement.select().fields(myFields).distinct().from(foo).build();</pre>
   *
   * @param fields The fields in the select clause
   * @param isDistinct Determines whether the DISTINCT clause should be added or not
   */
  public SelectStatement(Iterable<? extends AliasedFieldBuilder> fields, boolean isDistinct) {
    super(fields);
    if (AliasedField.immutableDslEnabled()) {
      this.groupBys = ImmutableList.of();
      this.setOperators = ImmutableList.of();
      this.hints = ImmutableList.of();
    } else {
      this.groupBys = Lists.newArrayList();
      this.setOperators = Lists.newArrayList();
      this.hints = Lists.newArrayList();
    }
    this.distinct = isDistinct;
  }


  /**
   * Checks whether the select statement should return a distinct set of results
   *
   * @return true if the statement should return a distinct set of results
   */
  public boolean isDistinct() {
    return distinct;
  }


  /**
   * @param selectFields Field references to select.
   * @deprecated Do not use {@link SelectStatement} mutably.  Create a new statement.
   */
  @Deprecated
  public final void appendFields(AliasedFieldBuilder... selectFields) {
    addFields(selectFields);
  }


  @Override
  public SelectStatementBuilder shallowCopy() {
    return new SelectStatementBuilder(this);
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
   * @return a new select statement with the change applied.
   */
  public SelectStatement groupBy(AliasedFieldBuilder field, AliasedFieldBuilder... otherFields) {
    return copyOnWriteOrMutate(
        (SelectStatementBuilder b) -> b.groupBy(field, otherFields),
        () -> {
          if (field == null) {
            throw new IllegalArgumentException("Field was null in group by clause");
          }

          // Add the singleton
          groupBys.add(field.build());

          // Add the list
          groupBys.addAll(Builder.Helper.buildAll(Lists.newArrayList(otherFields)));
        }
    );
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
   * @return a new select statement with the change applied.
   */
  public SelectStatement having(Criterion criterion) {
    return copyOnWriteOrMutate(
        (SelectStatementBuilder b) -> b.having(criterion),
        () -> {
          if (criterion == null) {
            throw new IllegalArgumentException("Criterion was null in having clause");
          }

          if (having != null) {
            throw new UnsupportedOperationException("Cannot specify more than one having clause per statement");
          }

          // Add the singleton
          having = criterion;
        }
    );
  }


  /**
   * Tells the database to pessimistically lock the tables.
   *
   * @return a new select statement with the change applied.
   */
  public SelectStatement forUpdate() {
    return copyOnWriteOrMutate(
        (SelectStatementBuilder b) -> b.forUpdate(),
        () -> this.forUpdate = true
    );
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
   * @return a new select statement with the change applied.
   */
  public SelectStatement optimiseForRowCount(int rowCount) {
    return copyOnWriteOrMutate(
        (SelectStatementBuilder b) -> b.optimiseForRowCount(rowCount),
        () -> this.hints.add(new OptimiseForRowCount(rowCount))
    );
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
   * @return a new select statement with the change applied.
   */
  public SelectStatement useIndex(TableReference table, String indexName) {
    return copyOnWriteOrMutate(
        (SelectStatementBuilder b) -> b.useIndex(table, indexName),
        () -> this.hints.add(new UseIndex(table, indexName))
    );
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
  public SelectStatement withParallelQueryPlan() {
    return copyOnWriteOrMutate(
      (SelectStatementBuilder b) -> b.withParallelQueryPlan(),
      () -> this.hints.add(new ParallelQueryHint())
  );
  }


  /**
   * This allows a custom hint to be passed in as a string to be used by the query
   */
  public org.alfasoftware.morf.sql.SelectStatement withCustomHint(String customHint) {
      return copyOnWriteOrMutate(
              (SelectStatementBuilder b) -> b.withCustomHint(customHint),
              () -> this.hints.add(new CustomHint(customHint))
      );
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
   * @return a new select statement with the change applied.
   */
  public SelectStatement useImplicitJoinOrder() {
    return copyOnWriteOrMutate(
        (SelectStatementBuilder b) -> b.useImplicitJoinOrder(),
        () -> this.hints.add(new UseImplicitJoinOrder())
    );
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
   * @return a new select statement with the change applied.
   */
  public SelectStatement union(SelectStatement selectStatement) {
    return copyOnWriteOrMutate(
        (SelectStatementBuilder b) -> b.union(selectStatement),
        () ->  setOperators.add(new UnionSetOperator(UnionStrategy.DISTINCT, this, selectStatement))
    );
  }


  /**
   * Perform an UNION set operation with another {@code selectStatement},
   * keeping all duplicate rows.
   *
   * @param selectStatement the other select statement to be united with the current select statement;
   * @return a new select statement with the change applied.
   * @see #union(SelectStatement)
   */
  public SelectStatement unionAll(SelectStatement selectStatement) {
    return copyOnWriteOrMutate(
        (SelectStatementBuilder b) -> b.unionAll(selectStatement),
        () ->  setOperators.add(new UnionSetOperator(UnionStrategy.ALL, this, selectStatement))
    );
  }


  /**
   * Gets the grouped fields
   *
   * @return the group by fields
   */
  public List<AliasedField> getGroupBys() {
    return groupBys;
  }


  /**
   * Gets the grouping filter
   *
   * @return the having fields
   */
  public Criterion getHaving() {
    return having;
  }


  /**
   * @return true if the statement should pessimistic lock the tables.
   */
  public boolean isForUpdate() {
    return forUpdate;
  }


  /**
   * @return all hints in the order they were declared.
   */
  public List<Hint> getHints() {
    return hints;
  }


  /**
   * @return the list of set operators to be applied on this select statement.
   */
  public List<SetOperator> getSetOperators() {
    return setOperators;
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.sql.AbstractSelectStatement#deepCopy()
   */
  @Override
  public SelectStatement deepCopy() {
    return deepCopy(noTransformation()).build();
  }


  /**
   * @see org.alfasoftware.morf.sql.AbstractSelectStatement#asField()
   */
  @Override
  public AliasedField asField() {
    return new FieldFromSelect(this);
  }


  /**
   * @see org.alfasoftware.morf.sql.AbstractSelectStatement#toString()
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("SQL SELECT ");
    if (distinct) {
      result.append("DISTINCT ");
    }
    if (!hints.isEmpty()) {
      result.append("with hints: ");
      result.append(hints);
      result.append(" ");
    }
    result.append(super.toString());
    if (!groupBys.isEmpty()) result.append(" GROUP BY ").append(groupBys);
    if (having != null) result.append(" HAVING [").append(having).append("]");
    for (SetOperator setOperator : setOperators) {
      result.append(" ").append(setOperator);
    }
    if (StringUtils.isNotEmpty(getAlias())) result.append(" AS ").append(getAlias());
    if (forUpdate) result.append(" (FOR UPDATE)");
    return result.toString();
  }


  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = generateHashCode();
    }
    return hashCode;
  }


  private int generateHashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (distinct ? 1231 : 1237);
    result = prime * result + (forUpdate ? 1231 : 1237);
    result = prime * result + (groupBys == null ? 0 : groupBys.hashCode());
    result = prime * result + (having == null ? 0 : having.hashCode());
    result = prime * result + (hints == null ? 0 : hints.hashCode());
    result = prime * result + (setOperators == null ? 0 : setOperators.hashCode());
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
    if (hashCode() != obj.hashCode())
      return false;
    if (!super.equals(obj))
      return false;
    SelectStatement other = (SelectStatement) obj;
    if (distinct != other.distinct)
      return false;
    if (forUpdate != other.forUpdate)
      return false;
    if (groupBys == null) {
      if (other.groupBys != null)
        return false;
    } else if (!groupBys.equals(other.groupBys))
      return false;
    if (having == null) {
      if (other.having != null)
        return false;
    } else if (!having.equals(other.having))
      return false;
    if (hints == null) {
      if (other.hints != null)
        return false;
    } else if (!hints.equals(other.hints))
      return false;
    if (setOperators == null) {
      if (other.setOperators != null)
        return false;
    } else if (!setOperators.equals(other.setOperators))
      return false;
    return true;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    super.drive(traverser);
    traverser
      .dispatch(groupBys)
      .dispatch(having)
      .dispatch(setOperators)
      .dispatch(hints);
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public SelectStatementBuilder deepCopy(DeepCopyTransformation transformer) {
    return new SelectStatementBuilder(this, transformer);
  }
}