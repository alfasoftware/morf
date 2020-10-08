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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.JoinType;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;

import com.google.common.collect.Iterables;

/**
 * Base builder for select statements.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 *
 * @param <U> The statement type.
 * @param <T> The builder type.
 */
public abstract class AbstractSelectStatementBuilder<U extends AbstractSelectStatement<U>, T extends AbstractSelectStatementBuilder<U, T>> implements Builder<U> {

  private final List<AliasedField>    fields = new ArrayList<>();
  private TableReference              table;
  private final List<SelectStatement> fromSelects = new ArrayList<>();
  private final List<Join>            joins = new ArrayList<>();
  private Criterion                   whereCriterion;
  private String                      alias;
  private final List<AliasedField>    orderBys = new ArrayList<>();


  protected AbstractSelectStatementBuilder() {
  }


  protected AbstractSelectStatementBuilder(AbstractSelectStatement<U> copyOf) {
    this.table = copyOf.getTable();
    this.whereCriterion = copyOf.getWhereCriterion();
    this.alias = copyOf.getAlias();
    this.fromSelects.addAll(copyOf.getFromSelects());
    this.joins.addAll(copyOf.getJoins());
    this.orderBys.addAll(copyOf.getOrderBys());
    this.fields.addAll(copyOf.getFields());
  }


  protected AbstractSelectStatementBuilder(AbstractSelectStatement<U> copyOf, DeepCopyTransformation transformation) {
    this.alias = copyOf.getAlias();
    this.table = transformation.deepCopy(copyOf.getTable());
    this.whereCriterion = transformation.deepCopy(copyOf.getWhereCriterion());
    this.fields.addAll(transformIterable(copyOf.getFields(), transformation));
    this.joins.addAll(transformIterable(copyOf.getJoins(), transformation));
    this.fromSelects.addAll(transformIterable(copyOf.getFromSelects(), transformation));
    this.orderBys.addAll(transformIterable(copyOf.getOrderBys(), transformation));
  }


  /**
   * Gets the alias of this select statement.
   *
   * @return the alias
   */
  String getAlias() {
    return alias;
  }


  /**
   * Gets the first table
   *
   * @return the table
   */
  TableReference getTable() {
    return table;
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
   * @return the fromSelects
   */
  List<SelectStatement> getFromSelects() {
    return fromSelects;
  }


  /**
   * Gets the list of joined tables in the order they are joined
   *
   * @return the joined tables
   */
  List<Join> getJoins() {
    return joins;
  }


  /**
   * Gets the fields which the select is ordered by
   *
   * @return the order by fields
   */
  List<AliasedField> getOrderBys() {
    return orderBys;
  }


  /**
   * Sets the alias for this select statement. This is useful if you are
   * including multiple select statements in a single select (not to be confused
   * with a join) and wish to reference the select statement itself.
   *
   * @param alias the alias to set.
   * @return this, for method chaining.
   */
  public T alias(String alias) {
    this.alias = alias;
    return castToChild(this);
  }


  /**
   * Adds fields to the select list.
   *
   * @param fields The fields to add
   * @return this, for method chaining.
   */
  public T fields(Iterable<? extends AliasedFieldBuilder> fields) {
    Iterables.addAll(this.fields, Builder.Helper.buildAll(fields));
    return castToChild(this);
  }


  /**
   * Adds fields to the select list.
   *
   * @param fields The fields to add
   * @return this, for method chaining.
   */
  public T fields(AliasedFieldBuilder... fields) {
    return fields(Arrays.asList(fields));
  }


  /**
   * Selects fields from a specific table:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from(tableRef(&quot;Foo&quot;));
   * </pre></blockquote>
   *
   * @param fromTable the table to select from
   * @return this, for method chaining.
   */
  public T from(TableReference fromTable) {
    this.table = fromTable;
    return castToChild(this);
  }


  /**
   * Selects fields from a specific table:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from(&quot;Foo&quot;);
   * </pre></blockquote>
   *
   * @param tableName the table to select from
   * @return this, for method chaining.
   */
  public T from(String tableName) {
    return from(tableRef(tableName));
  }


  /**
   * Selects fields from one or more inner selects:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from(select().from(&quot;Foo&quot;));
   * </pre></blockquote>
   *
   * @param fromSelect the select statements to select from
   * @return this, for method chaining.
   */
  public T from(SelectStatement... fromSelect) {
    fromSelects.addAll(Arrays.asList(fromSelect));
    return castToChild(this);
  }


  /**
   * Specifies a table to join to.
   *
   * <blockquote><pre>
   * TableReference foo = tableRef("Foo");
   * TableReference bar = tableRef("Bar");
   * SelectStatement statement = select()
   *     .from(foo)
   *     .innerJoin(bar, foo.field(&quot;id&quot;).eq(bar.field(&quot;fooId&quot;)));</pre>
   * </blockquote>
   *
   * @param toTable the table to join to
   * @param criterion the criteria on which to join the tables
   * @return this, for method chaining.
   */
  public T innerJoin(TableReference toTable, Criterion criterion) {
    joins.add(new Join(JoinType.INNER_JOIN, toTable, criterion));
    return castToChild(this);
  }


  /**
   * Specifies a table to cross join to (creating a cartesian product).
   *
   * <blockquote><pre>
   * TableReference foo = tableRef("Foo");
   * TableReference bar = tableRef("Bar");
   * SelectStatement statement = select()
   *     .from(tableRef("Foo"))
   *     .innerJoin(tableRef("Bar"));</pre>
   * </blockquote>
   *
   * @param toTable the table to join to
   * @return this, for method chaining.
   */
  public T crossJoin(TableReference toTable) {
    joins.add(new Join(JoinType.INNER_JOIN, toTable, null));
    return castToChild(this);
  }


  /**
   * @deprecated Use {@link #crossJoin(TableReference)} to do a cross join;
   *             or add join conditions for {@link #innerJoin(TableReference, Criterion)
   *             to make this an inner join.
   */
  @Deprecated
  public T innerJoin(TableReference toTable) {
    joins.add(new Join(JoinType.INNER_JOIN, toTable, null));
    return castToChild(this);
  }


  /**
   * Specifies an inner join to a subselect:
   *
   * <blockquote><pre>
   * TableReference sale = tableRef("Sale");
   * TableReference customer = tableRef("Customer");
   *
   * // Define the subselect - a group by showing total sales by age
   * SelectStatement amountsByAge = select(field("age"), sum(field("amount")))
   *     .from(sale)
   *     .innerJoin(customer, sale.field("customerId").eq(customer.field("id")))
   *     .groupBy(customer.field("age")
   *     .alias("amountByAge");
   *
   * // The outer select, showing each sale as a percentage of the sales to that age
   * SelectStatement outer = select(
   *       sale.field("id"),
   *       sale.field("amount")
   *          .divideBy(amountByAge.asTable().field("amount"))
   *          .multiplyBy(literal(100))
   *     )
   *     .from(sale)
   *     .innerJoin(customer, sale.field("customerId").eq(customer.field("id")))
   *     .innerJoin(amountsByAge, amountsByAge.asTable().field("age").eq(customer.field("age")));
   * </pre></blockquote>
   *
   * @param subSelect the sub select statement to join on to
   * @param onCondition the criteria on which to join the tables
   * @return this, for method chaining.
   */
  public T innerJoin(SelectStatement subSelect, Criterion onCondition) {
    joins.add(new Join(JoinType.INNER_JOIN, subSelect, onCondition));
    return castToChild(this);
  }


  /**
   * Specifies a cross join (creating a cartesian product) to a subselect:
   *
   * <blockquote><pre>
   * // Each sale as a percentage of all sales
   * TableReference sale = tableRef("Sale");
   * SelectStatement outer = select(
   *       sale.field("id"),
   *       sale.field("amount")
   *          .divideBy(totalSales.asTable().field("amount"))
   *          .multiplyBy(literal(100))
   *     )
   *     .from(sale)
   *     .innerJoin(
   *       select(sum(field("amount")))
   *           .from(sale)
   *           .alias("totalSales")
   *     );
   * </pre></blockquote>
   *
   * @param subSelect the sub select statement to join on to
   * @return this, for method chaining.
   */
  public T crossJoin(SelectStatement subSelect) {
    joins.add(new Join(JoinType.INNER_JOIN, subSelect, null));
    return castToChild(this);
  }


  /**
   * @deprecated Use {@link #crossJoin(SelectStatement)} to do a cross join;
   *             or add join conditions for {@link #innerJoin(SelectStatement, Criterion)
   *             to make this an inner join.
   */
  @Deprecated
  public T innerJoin(SelectStatement subSelect) {
    joins.add(new Join(JoinType.INNER_JOIN, subSelect, null));
    return castToChild(this);
  }


  /**
   * Specifies a left outer join to a table:
   *
   * <blockquote><pre>
   * TableReference foo = tableRef("Foo");
   * TableReference bar = tableRef("Bar");
   * SelectStatement statement = select()
   *     .from(foo)
   *     .leftOuterJoin(bar, foo.field(&quot;id&quot;).eq(bar.field(&quot;fooId&quot;)));</pre>
   * </blockquote>
   *
   * @param toTable the table to join to
   * @param criterion the criteria on which to join the tables
   * @return this, for method chaining.
   */
  public T leftOuterJoin(TableReference toTable, Criterion criterion) {
    joins.add(new Join(JoinType.LEFT_OUTER_JOIN, toTable, criterion));
    return castToChild(this);
  }


  /**
   * Specifies an left outer join to a subselect:
   *
   * <blockquote><pre>
   * TableReference sale = tableRef("Sale");
   * TableReference customer = tableRef("Customer");
   *
   * // Define the subselect - a group by showing total sales by age in the
   * // previous month.
   * SelectStatement amountsByAgeLastMonth = select(field("age"), sum(field("amount")))
   *     .from(sale)
   *     .innerJoin(customer, sale.field("customerId").eq(customer.field("id")))
   *     .where(sale.field("month").eq(5))
   *     .groupBy(customer.field("age")
   *     .alias("amountByAge");
   *
   * // The outer select, showing each sale this month as a percentage of the sales
   * // to that age the previous month
   * SelectStatement outer = select(
   *       sale.field("id"),
   *       sale.field("amount")
   *          // May cause division by zero (!)
   *          .divideBy(isNull(amountsByAgeLastMonth.asTable().field("amount"), 0))
   *          .multiplyBy(literal(100))
   *     )
   *     .from(sale)
   *     .innerJoin(customer, sale.field("customerId").eq(customer.field("id")))
   *     .leftOuterJoin(amountsByAgeLastMonth, amountsByAgeLastMonth.asTable().field("age").eq(customer.field("age")));
   * </pre></blockquote>
   *
   * @param subSelect the sub select statement to join on to
   * @param criterion the criteria on which to join the tables
   * @return this, for method chaining.
   */
  public T leftOuterJoin(SelectStatement subSelect, Criterion criterion) {
    joins.add(new Join(JoinType.LEFT_OUTER_JOIN, subSelect, criterion));
    return castToChild(this);
  }


  /**
   * Specifies the where criteria:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from("Foo").where(field("name").eq("Dave"));
   * </pre></blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return this, for method chaining.
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
   * @return this, for method chaining.
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
   * Specifies the fields by which to order the result set:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from("Foo").orderBy(field("name").desc());
   * </pre></blockquote>
   *
   * @param orderFields the fields to order by
   * @return this, for method chaining.
   */
  public T orderBy(AliasedFieldBuilder... orderFields) {
    if (orderFields == null) {
      throw new IllegalArgumentException("Fields were null in order by clause");
    }
    return orderBy(Arrays.asList(orderFields));
  }


  /**
   * Specifies the fields by which to order the result set. For use in builder code.
   * See {@link #orderBy(AliasedFieldBuilder...)} for the DSL version.
   *
   * @param orderFields the fields to order by
   * @return this, for method chaining.
   */
  public T orderBy(Iterable<? extends AliasedFieldBuilder> orderFields) {
    if (orderFields == null) {
      throw new IllegalArgumentException("Fields were null in order by clause");
    }

    if(AliasedField.immutableDslEnabled()) {
      Iterables.addAll(orderBys, SqlInternalUtils.transformOrderByToAscending(Builder.Helper.buildAll(orderFields)));
    } else {
      // Add the list
      Iterables.addAll(orderBys, Builder.Helper.buildAll(orderFields));
      // Default fields to ascending if no direction has been specified
      SqlInternalUtils.defaultOrderByToAscending(orderBys);
    }
    return castToChild(this);
  }


  /**
   * @param abstractSelectStatement
   * @return
   */
  @SuppressWarnings("unchecked")
  private T castToChild(AbstractSelectStatementBuilder<U, T> abstractSelectStatement) {
    return (T) abstractSelectStatement;
  }
}