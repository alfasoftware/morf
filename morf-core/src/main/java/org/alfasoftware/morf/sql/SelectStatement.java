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
import java.util.List;

import org.alfasoftware.morf.sql.UnionSetOperator.UnionStrategy;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

import com.google.common.collect.Lists;

/**
 * Class which encapsulates the generation of an SELECT SQL statement.
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 *   new SelectStatement([field], ...)
 *        |----&gt; .from([table])                               = SELECT [fields] FROM [table]
 *                |----&gt; .innerJoin([table], [criterion])     = SELECT [fields] FROM [table] INNER JOIN [table] ON [criterion]
 *                |----&gt; .leftOuterJoin(...)
 *                |----&gt; .where([criterion])                  = SELECT [fields] FROM [table] WHERE [criterion]
 *                |----&gt; .orderBy([fields])                   = SELECT [fields] FROM [table] ORDER BY [fields]
 *                |----&gt; .groupBy([fields])                   = SELECT [fields] FROM [table] GROUP BY [fields]
 *                        |----&gt; having([criterion])          = SELECT [fields] FROM [table] GROUP BY [fields] HAVING [criterion]
 *                |----&gt; .union([SelectStatement])            = SELECT [fields] FROM [table] UNION [SelectStatement]
 *                |----&gt; .unionAll([SelectStatement])         = SELECT [fields] FROM [table] UNION ALL [SelectStatement]
 *  </pre></blockquote>
 *
 * <p>This class does not accept string references to field or table names. Instead, you must provide
 * the methods with a {@link TableReference} or {@link FieldReference} reference.</p>
 *
 * <p>Each method of this class will return an instance of the SelectStatement class. However, this will always
 * be the same instance rather than a new instance of the class.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class SelectStatement extends AbstractSelectStatement<SelectStatement> implements DeepCopyableWithTransformation<SelectStatement,Builder<SelectStatement>> ,Driver  {

  /**
   * Indicates whether the select statement is required to return a distinct set of results
   */
  private final boolean distinct;

  /**
   * The fields to group together
   */
  private final List<AliasedField> groupBys = new ArrayList<>();

  /**
   * The criteria to use for the group selection
   */
  private Criterion          having;

  /**
   * The set operators to merge with the result set.
   */
  private final List<SetOperator> setOperators = new ArrayList<>();

  /**
   * WITH LOCK
   */
  private boolean forUpdate;

  /**
   * Lists the declared hints in the order they were declared.
   */
  private final List<Hint> hints = new ArrayList<>();


  /**
   * Constructor used to create a deep copy for a select statement
   *
   * @param sourceStatement the select statement to create the deep copy from
   */
  SelectStatement(SelectStatement sourceStatement,DeepCopyTransformation transformation) {
    super(sourceStatement,transformation);
    this.distinct = sourceStatement.distinct;
    this.having = transformation.deepCopy(sourceStatement.having);
    this.groupBys.addAll(DeepCopyTransformations.transformIterable(sourceStatement.groupBys, transformation));
    this.setOperators.addAll(DeepCopyTransformations.transformIterable(sourceStatement.setOperators,transformation));
  }


  /**
   * Constructs a Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all fields (i.e. "SELECT * FROM x")
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
   * @param fields an array of fields that should be selected
   */
  public SelectStatement(List<? extends AliasedFieldBuilder> fields) {
    this(fields, false);
  }


  /**
   * Constructs a distinct Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all fields (i.e. "SELECT DISTINCT * FROM x")
   *
   * @param fields The fields in the select clause
   * @param isDistinct Determines whether the DISTINCT clause should be added or not
   */
  public SelectStatement(AliasedFieldBuilder[] fields, boolean isDistinct) {
    super();
    this.distinct = isDistinct;
    appendFields(fields);
  }


  /**
   * Constructs a distinct Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all fields (i.e. "SELECT DISTINCT * FROM x")
   *
   * @param fields The fields in the select clause
   * @param isDistinct Determines whether the DISTINCT clause should be added or not
   */
  public SelectStatement(List<? extends AliasedFieldBuilder> fields, boolean isDistinct) {
    super();
    this.distinct = isDistinct;
    addFields(fields);
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
   */
  public final void appendFields(AliasedFieldBuilder... selectFields) {
    addFields(selectFields);
  }


  /**
   * Specifies that the records should be grouped by the specified fields.
   *
   * <blockquote><pre>
   *    new SelectStatement().from(new Table("schedule"))
   *                         .groupBy(new Field("agreementnumber"));</pre></blockquote>
   *
   * @param field the first field to group by
   * @param otherFields the remaining fields to group by
   * @return the updated SelectStatement (this will not be a new object)
   */
  public SelectStatement groupBy(AliasedFieldBuilder field, AliasedFieldBuilder... otherFields) {
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
   * Filters the grouped records by some criteria.
   *
   * <blockquote><pre>
   *    new SelectStatement().from(new Table("schedule"))
   *                         .groupBy(new Field("agreementnumber"))
   *                         .having(Criteria.eq(new Field("agreementnumber"), "A0001"));</pre></blockquote>
   *
   * @param criterion the criteria on which to filter the grouped records
   * @return the updated SelectStatement (this will not be a new object)
   */
  public SelectStatement having(Criterion criterion) {
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
   * Tells the database to pessimistic lock the tables.
   *
   * @return the updated SelectStatement (this will not be a new object)
   */
  public SelectStatement forUpdate() {
    this.forUpdate = true;
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
   * @return the updated SelectStatement (this will not be a new object)
   */
  public SelectStatement optimiseForRowCount(int rowCount) {
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
   * @return the updated SelectStatement (this will not be a new object)
   */
  public SelectStatement useIndex(TableReference table, String indexName) {
    this.hints.add(new UseIndex(table, indexName));
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
   * @return the updated SelectStatement (this will not be a new object)
   */
  public SelectStatement useImplicitJoinOrder() {
    this.hints.add(new UseImplicitJoinOrder());
    return this;
  }


  /**
   * Perform an UNION set operation with another {@code selectStatement},
   * eliminating any duplicate rows.
   *
   * <p>It is possible to have more than one union statement by chaining union calls:</p>
   * <blockquote><pre>
   * SelectStatement stmtA = new SelectStatement(...).from(...);
   * SelectStatement stmtB = new SelectStatement(...).from(...);
   * SelectStatement stmtC = new SelectStatement(...).from(...).union(stmtA).union(stmtB).orderBy(...);
   * </pre></blockquote>
   *
   * <p>If an union operation is performed then all
   * participating select statements require the same selected column list, i.e.
   * same naming and ordering. In addition, only the leftmost select statement
   * should have an order-by statement (see example above).</p>
   *
   * @param selectStatement the select statement to be united with the current select statement;
   * @return the updated SelectStatement (this will not be a new object)
   */
  public SelectStatement union(SelectStatement selectStatement) {
    setOperators.add(new UnionSetOperator(UnionStrategy.DISTINCT, this, selectStatement));

    return this;
  }


  /**
   * Perform an UNION set operation with another {@code selectStatement},
   * keeping all duplicate rows.
   *
   * @param selectStatement the other select statement to be united with the current select statement;
   * @return the updated SelectStatement (this will not be a new object)
   * @see #union(SelectStatement)
   */
  public SelectStatement unionAll(SelectStatement selectStatement) {
    setOperators.add(new UnionSetOperator(UnionStrategy.ALL, this, selectStatement));

    return this;
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
    return new SelectStatement(this,noTransformation());
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
    result.append(super.toString());
    if (!groupBys.isEmpty()) result.append(" GROUP BY ").append(groupBys);
    if (having != null) result.append(" HAVING [").append(having).append("]");
    for (SetOperator setOperator : setOperators) {
      result.append(" ").append(setOperator);
    }
    if (forUpdate) result.append(" (FOR UPDATE)");
    return result.toString();
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
   super.drive(traverser);
   traverser
    .dispatch(getGroupBys())
    .dispatch(getHaving())
    .dispatch(getSetOperators());
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<SelectStatement> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new SelectStatement(this,transformer));
  }
}
