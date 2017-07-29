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

import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.util.DeepCopyTransformations.transformIterable;

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.Direction;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.JoinType;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * AbstractSelectStatement class for select statements.
 * <p>
 * This abstraction is aware of subclasses type.
 * </p>
 *
 * @see  SelectFirstStatement
 * @see SelectStatement
 * @author Copyright (c) Alfa Financial Software 2014
 */
public abstract class AbstractSelectStatement<T extends AbstractSelectStatement<T>>  implements Statement,Driver{

  /**
   * The fields to select from the table
   */
  private final List<AliasedField>    fields = Lists.newArrayList();

  /**
   * The primary table to select from
   */
  private TableReference              table;

  /**
   * Select from multiple inner selects.
   */
  private final List<SelectStatement> fromSelects  = Lists.newArrayList();

  /**
   * The additional tables to join to
   */
  private final List<Join>            joins  = Lists.newArrayList();

  /**
   * The selection criteria for selecting from the database
   */
  private Criterion                   whereCriterion;

  /**
   * The alias to associate with this select statement. Useful when combining
   * multiple select statements.
   */
  private String                      alias;

  /**
   * The fields to sort the result set by
   */
  private final List<AliasedField>    orderBys  = Lists.newArrayList();


  protected AbstractSelectStatement() {}

  protected AbstractSelectStatement(AliasedFieldBuilder... aliasedFields) {
    this.fields.addAll(Builder.Helper.<AliasedField>buildAll(Lists.newArrayList(aliasedFields)));
  }


  /**
   * Deep copy constructor
   * @param sourceStatement The source to copy from
   * @param transformation Intercepts the deep copy to transform instead of mere copying
   *
   */
  protected AbstractSelectStatement(AbstractSelectStatement<T> sourceStatement,DeepCopyTransformation transformation) {
    this.alias = sourceStatement.alias;
    this.table = transformation.deepCopy(sourceStatement.table);
    this.fields.addAll(transformIterable(sourceStatement.fields, transformation));
    this.whereCriterion = transformation.deepCopy(sourceStatement.whereCriterion);
    this.joins.addAll(transformIterable(sourceStatement.joins, transformation));
    this.fromSelects.addAll(transformIterable(sourceStatement.fromSelects, transformation));
    this.orderBys.addAll(transformIterable(sourceStatement.orderBys, transformation));
  }


  /**
   * @param aliasedFields The fields to add
   */
  protected void addFields(AliasedFieldBuilder... aliasedFields) {
    addFields(Arrays.asList(aliasedFields));
  }


  /**
   * @param aliasedFields The fields to add
   */
  protected void addFields(List<? extends AliasedFieldBuilder> aliasedFields) {
    fields.addAll(FluentIterable.from(aliasedFields)
      .transform(Builder.Helper.<AliasedField>buildAll()).toList());
  }


  /**
   * Gets the first table
   *
   * @return the table
   */
  public TableReference getTable() {
    return table;
  }


  /**
   * Gets the list of fields
   *
   * @return the fields
   */
  public List<AliasedField> getFields() {
    return fields;
  }


  /**
   * @return the fromSelects
   */
  public List<SelectStatement> getFromSelects() {
    return fromSelects;
  }


  /**
   * Gets the list of joined tables in the order they are joined
   *
   * @return the joined tables
   */
  public List<Join> getJoins() {
    return joins;
  }


  /**
   * Gets the where criteria.
   *
   * @return the where criteria
   */
  public Criterion getWhereCriterion() {
    return whereCriterion;
  }


  /**
   * Gets the fields which the select is ordered by
   *
   * @return the order by fields
   */
  public List<AliasedField> getOrderBys() {
    return orderBys;
  }


  /**
   * @return this select statement as a sub-select defining a field.
   */
  public abstract AliasedField asField();


  /**
   * @return a reference to the alias of the select statement.
   */
  public TableReference asTable() {
    return new TableReference(getAlias());
  }


  /**
   * Gets the alias of this select statement.
   *
   * @return the alias
   */
  public String getAlias() {
    return alias;
  }


  /**
   * Sets the alias for this select statement. This is useful if you are
   * including multiple select statements in a single select (not to be confused
   * with a join) and wish to reference the select statement itself. c.f.
   * WEB-12490
   *
   * @param alias the alias to set.
   * @return the select statement to allow chaining.
   */
  public T alias(String alias) {
    this.alias = alias;

    return castToChild(this);
  }


  /**
   * Selects fields from a specific table. <blockquote>
   *
   * <pre>
   * newT().from(new Table(&quot;agreement&quot;));
   * </pre>
   *
   * </blockquote>
   *
   * @param fromTable the table to select from
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T from(TableReference fromTable) {
    this.table = fromTable;
    return castToChild(this);
  }


  /**
   * Selects fields from a specific table. <blockquote>
   *
   * <pre>
   * new SelectStatement().from(&quot;Agreement&quot;);
   * </pre>
   *
   * </blockquote>
   *
   * @param tableName the table to select from
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T from(String tableName) {
    this.table = tableRef(tableName);
    return castToChild(this);
  }


  /**
   * Selects fields from one or more inner selects. <blockquote>
   *
   * <pre>
   *    new SelectStatement().from(new SelectStatement(...));
   * </pre>
   *
   * </blockquote>
   *
   * @param fromSelect the select statements to select from
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T from(SelectStatement... fromSelect) {
    fromSelects.addAll(Arrays.asList(fromSelect));
    return castToChild(this);
  }


  /**
   * Specifies a table to join to <blockquote>
   *
   * <pre>
   * new SelectStatement().from(new Table(&quot;agreement&quot;)).innerJoin(new Table(&quot;schedule&quot;),
   *   Criteria.eq(new Field(&quot;agreementnumber&quot;), &quot;A0001&quot;));
   * </pre>
   *
   * </blockquote>
   *
   * @param toTable the table to join to
   * @param criterion the criteria on which to join the tables
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T innerJoin(TableReference toTable, Criterion criterion) {
    joins.add(new Join(JoinType.INNER_JOIN, toTable).on(criterion));
    return castToChild(this);
  }


  /**
   * Specifies a table to join to <blockquote>
   *
   * <pre>
   * new SelectStatement().from(new Table(&quot;agreement&quot;)).innerJoin(new Table(&quot;schedule&quot;),
   *   Criteria.eq(new Field(&quot;agreementnumber&quot;), &quot;A0001&quot;));
   * </pre>
   *
   * </blockquote>
   *
   * @param toTable the table to join to
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T innerJoin(TableReference toTable) {
    joins.add(new Join(JoinType.INNER_JOIN, toTable));
    return castToChild(this);
  }


  /**
   * Specifies a table to join to <blockquote>
   *
   * <pre>
   *   SelectStatement previousSystemDate = select(field("dataValues").asDate().as("previousSystemDate"))
   *                                       .from("DataArea")
   *                                       .where(and(field("dataAreaName").eq("CHPDATDTA"),
   *                                                  field("dataValueStart").eq("217")));
   *
   *   select(field("daysPassedDue")).from("ScheduleInvoiceDelinquency")
   *                                 .innerJoin(select(daysBetween(field("previousSystemDate"), field("invoiceDueDate")).as("daysPassedDue"))
   *                                             .from(invoice)
   *                                             .innerJoin(previousSystemDate),
   *                                            field("invoiceId").eq(field("id")))
   * </pre>
   *
   * </blockquote>
   *
   * @param subSelect the sub select statement to join on to
   * @param onCondition the criteria on which to join the tables
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T innerJoin(SelectStatement subSelect, Criterion onCondition) {
    joins.add(new Join(JoinType.INNER_JOIN, subSelect).on(onCondition));
    return castToChild(this);
  }


  /**
   * Specifies a table to join to <blockquote>
   *
   * <pre>
   * SelectStatement previousSystemDate = select(field(&quot;dataValues&quot;).as(&quot;previousSystemDate&quot;)).from(&quot;DataArea&quot;)
   *     .where(and(field(&quot;dataAreaName&quot;).eq(&quot;CHPDATDTA&quot;), field(&quot;dataValueStart&quot;).eq(&quot;217&quot;))).alias(&quot;D&quot;);
   *
   * select(field(&quot;invoiceDueDate&quot;), field(&quot;previousSystemDate&quot;)).from(&quot;Invoice&quot;).innerJoin(previousSystemDate);
   * </pre>
   *
   * </blockquote>
   *
   * @param subSelect the sub select statement to join on to
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T innerJoin(SelectStatement subSelect) {
    joins.add(new Join(JoinType.INNER_JOIN, subSelect));
    return castToChild(this);
  }


  /**
   * Specifies a table to join to <blockquote>
   *
   * <pre>
   * new SelectStatement().from(new Table(&quot;agreement&quot;)).leftOuterJoin(new Table(&quot;schedule&quot;),
   *   Criteria.eq(new Field(&quot;agreementnumber&quot;), &quot;A0001&quot;));
   * </pre>
   *
   * </blockquote>
   *
   * @param toTable the table to join to
   * @param criterion the criteria on which to join the tables
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T leftOuterJoin(TableReference toTable, Criterion criterion) {
    joins.add(new Join(JoinType.LEFT_OUTER_JOIN, toTable).on(criterion));
    return castToChild(this);
  }


  /**
   * Specifies a table to join to <blockquote>
   *
   * <pre>
   * SelectStatement previousSystemDate = select(field(&quot;dataValues&quot;).as(&quot;previousSystemDate&quot;)).from(&quot;DataArea&quot;)
   *     .where(and(field(&quot;dataAreaName&quot;).eq(&quot;CHPDATDTA&quot;), field(&quot;dataValueStart&quot;).eq(&quot;217&quot;))).alias(&quot;D&quot;);
   *
   * select(field(&quot;invoiceDueDate&quot;)).from(&quot;Invoice&quot;).leftOuterJoin(previousSystemDate,
   *   eq(field(&quot;invoiceDueDate&quot;), field(&quot;previousSystemDate&quot;)));
   * </pre>
   *
   * </blockquote>
   *
   * @param subSelect the sub select statement to join on to
   * @param criterion the criteria on which to join the tables
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T leftOuterJoin(SelectStatement subSelect, Criterion criterion) {
    joins.add(new Join(JoinType.LEFT_OUTER_JOIN, subSelect).on(criterion));
    return castToChild(this);
  }


  /**
   * Specifies the where criteria <blockquote>
   *
   * <pre>
   * new SelectStatement().from(new Table(&quot;agreement&quot;)).where(Criteria.eq(new Field(&quot;agreementnumber&quot;), &quot;A0001&quot;));
   * </pre>
   *
   * </blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T where(Criterion criterion) {
    if (criterion == null) {
      throw new IllegalArgumentException("Criterion was null in where clause");
    }

    whereCriterion = criterion;

    return castToChild(this);
  }


  /**
   * Specifies the where criteria. For use in code where the criteria are being generated dynamically.
   * The iterable can be empty but not null.
   *
   * @param criteria the criteria to filter the results by. They will be <i>AND</i>ed together.
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T where(Iterable<Criterion> criteria) {
    if (criteria == null) {
      throw new IllegalArgumentException("No criterion was given in the where clause");
    }

    if (!Iterables.isEmpty(criteria)) {
      whereCriterion = new Criterion(Operator.AND, criteria);
    }

    return castToChild(this);
  }


  /**
   * Specifies the fields by which to order the result set. <blockquote>
   *
   * <pre>
   * new SelectStatement().from(new Table(&quot;schedule&quot;)).orderBy(new Field(&quot;agreementnumber&quot;, Direction.DESCENDING));
   * </pre>
   *
   * </blockquote>
   *
   * @param orderFields the fields to order by
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T orderBy(AliasedField... orderFields) {
    if (orderFields == null) {
      throw new IllegalArgumentException("Fields were null in order by clause");
    }
    return orderBy(Arrays.asList(orderFields));
  }


  /**
   * Specifies the fields by which to order the result set. For use in builder code.
   * See {@link #orderBy(AliasedField...)} for the DSL version.
   *
   * @param orderFields the fields to order by
   * @return the updated SelectStatement (this will not be a new object)
   */
  public T orderBy(Iterable<AliasedField> orderFields) {
    if (orderFields == null) {
      throw new IllegalArgumentException("Fields were null in order by clause");
    }

    // Add the list
    Iterables.addAll(orderBys, orderFields);

    // Default fields to ascending if no direction has been specified
    for (AliasedField currentField : orderBys) {
      if (currentField instanceof FieldReference && ((FieldReference) currentField).getDirection() == Direction.NONE) {
        ((FieldReference) currentField).setDirection(Direction.ASCENDING);
      }
    }

    return castToChild(this);
  }


  /**
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  public abstract T deepCopy();


  /**
   * @param abstractSelectStatement
   * @return
   */
  @SuppressWarnings("unchecked")
  private T castToChild(AbstractSelectStatement<T> abstractSelectStatement) {
    return (T) abstractSelectStatement;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(org.alfasoftware.morf.util.ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser dispatcher) {
    dispatcher
    .dispatch(getTable())
    .dispatch(getFields())
    .dispatch(getJoins())
    .dispatch(getFromSelects())
    .dispatch(getWhereCriterion());
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {

    StringBuilder result = new StringBuilder();
    result.append(fields);

    result.append(" FROM [");
    if (table != null) result.append(table);
    boolean first = true;
    for (SelectStatement select : fromSelects) {
      if (first) result.append(", ");
      result.append("(").append(select).append(")");
      first = false;
    }
    result.append("]");

    if (!joins.isEmpty()) result.append(" ");
    result.append(StringUtils.join(joins, " "));

    if (whereCriterion != null) result.append(" WHERE [").append(whereCriterion).append("]");
    if (!orderBys.isEmpty()) result.append(" ORDER BY ").append(orderBys);
    return result.toString();
  }
}