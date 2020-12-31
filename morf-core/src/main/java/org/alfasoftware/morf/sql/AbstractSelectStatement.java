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

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.JoinType;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Common behaviour of {@link SelectStatement} and {@link SelectFirstStatement}.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public abstract class AbstractSelectStatement<T extends AbstractSelectStatement<T>>  implements Statement,Driver{

  /**
   * The fields to select from the table
   */
  private final List<AliasedField>    fields;

  /**
   * TODO make final
   *
   * The primary table to select from
   */
  private TableReference              table;

  /**
   * Select from multiple inner selects.
   */
  private final List<SelectStatement> fromSelects;

  /**
   * The additional tables to join to
   */
  private final List<Join>            joins;

  /**
   * TODO make final
   *
   * The selection criteria for selecting from the database
   */
  private Criterion                   whereCriterion;

  /**
   * TODO make final
   *
   * The alias to associate with this select statement. Useful when combining
   * multiple select statements.
   */
  private String                      alias;

  /**
   * The fields to sort the result set by
   */
  private final List<AliasedField>    orderBys;


  protected AbstractSelectStatement(Iterable<? extends AliasedFieldBuilder> aliasedFields) {
    if (AliasedField.immutableDslEnabled()) {
      this.fromSelects = ImmutableList.of();
      this.joins = ImmutableList.of();
      this.orderBys = ImmutableList.of();
      this.fields = FluentIterable.from(aliasedFields).transform(AliasedFieldBuilder::build).toList();
    } else {
      this.fromSelects = Lists.newArrayList();
      this.joins = Lists.newArrayList();
      this.orderBys = Lists.newArrayList();
      this.fields = Lists.newArrayList();
      this.fields.addAll(Builder.Helper.<AliasedField>buildAll(aliasedFields));
    }
  }


  protected AbstractSelectStatement(AliasedFieldBuilder... aliasedFields) {
    this(Arrays.asList(aliasedFields));
  }


  /**
   * Constructor for use when using the builder.
   *
   * @param builder The builder.
   */
  AbstractSelectStatement(AbstractSelectStatementBuilder<T, ?> builder) {
    this.table = builder.getTable();
    this.whereCriterion = builder.getWhereCriterion();
    this.alias = builder.getAlias();
    if (AliasedField.immutableDslEnabled()) {
      this.fromSelects = ImmutableList.copyOf(builder.getFromSelects());
      this.joins = ImmutableList.copyOf(builder.getJoins());
      this.orderBys = ImmutableList.copyOf(builder.getOrderBys());
      this.fields = ImmutableList.copyOf(builder.getFields());
    } else {
      this.fromSelects = Lists.newArrayList(builder.getFromSelects());
      this.joins = Lists.newArrayList(builder.getJoins());
      this.orderBys = Lists.newArrayList(builder.getOrderBys());
      this.fields = Lists.newArrayList(builder.getFields());
    }
  }


  /**
   * @param aliasedFields The fields to add
   * @deprecated Do not use {@link AbstractSelectStatement} mutably. Create a new statement.
   */
  @Deprecated
  protected void addFields(AliasedFieldBuilder... aliasedFields) {
    addFields(Arrays.asList(aliasedFields));
  }


  /**
   * @param aliasedFields The fields to add
   * @deprecated Do not use {@link AbstractSelectStatement} mutably. Create a new statement.
   */
  @Deprecated
  protected void addFields(Iterable<? extends AliasedFieldBuilder> aliasedFields) {
    AliasedField.assetImmutableDslDisabled();
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
   * with a join) and wish to reference the select statement itself.
   *
   * @param alias the alias to set.
   * @return the new select statement with the change applied.
   */
  public T alias(String alias) {
    return copyOnWriteOrMutate(
        b -> b.alias(alias),
        () -> this.alias = alias
    );
  }


  /**
   * Selects fields from a specific table:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from(tableRef(&quot;Foo&quot;));
   * </pre></blockquote>
   *
   * @param fromTable the table to select from
   * @return a new select statement with the change applied.
   */
  public T from(TableReference fromTable) {
    return copyOnWriteOrMutate(
        b -> b.from(fromTable),
        () -> this.table = fromTable
    );
  }


  /**
   * Either uses {@link #shallowCopy()} and mutates the result, returning it,
   * or mutates the statement directly, depending on
   * {@link AliasedField#immutableDslEnabled()}.
   *
   * TODO for removal along with mutable behaviour.
   *
   * @param transform A transform which modifies the shallow copy builder.
   * @param mutator Code which applies the local changes instead.
   * @param <U> The builder type.
   * @return The result (which may be {@code this}).
   */
  @SuppressWarnings({ "unchecked" })
  protected <U extends AbstractSelectStatementBuilder<T, ?>> T copyOnWriteOrMutate(Function<U, U> transform, Runnable mutator) {
    if (AliasedField.immutableDslEnabled()) {
      return transform.apply((U) shallowCopy()).build();
    } else {
      mutator.run();
      return castToChild(this);
    }
  }


  /**
   * Performs a shallow copy to a builder, allowing a duplicate
   * to be created and modified.
   *
   * @return A builder, initialised as a duplicate of this statement.
   */
  public abstract AbstractSelectStatementBuilder<T, ?> shallowCopy();


  /**
   * Selects fields from a specific table:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from(&quot;Foo&quot;);
   * </pre></blockquote>
   *
   * @param tableName the table to select from
   * @return a new select statement with the change applied.
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
   * @return a new select statement with the change applied.
   */
  public T from(SelectStatement... fromSelect) {
    return copyOnWriteOrMutate(
        b -> b.from(fromSelect),
        () -> this.fromSelects.addAll(Arrays.asList(fromSelect))
    );
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
   * @return a new select statement with the change applied.
   */
  public T innerJoin(TableReference toTable, Criterion criterion) {
    return copyOnWriteOrMutate(
        b -> b.innerJoin(toTable, criterion),
        () -> joins.add(new Join(JoinType.INNER_JOIN, toTable, criterion))
    );
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
   * @return a new select statement with the change applied.
   */
  public T crossJoin(TableReference toTable) {
    return copyOnWriteOrMutate(
        b -> b.crossJoin(toTable),
        () -> joins.add(new Join(JoinType.INNER_JOIN, toTable, null))
    );
  }


  /**
   * @param toTable the table to join to
   * @return a new select statement with the change applied.
   *
   * @deprecated Use {@link #crossJoin(TableReference)} to do a cross join;
   *             or add join conditions for {@link #innerJoin(SelectStatement, Criterion)}
   *             to make this an inner join.
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  public T innerJoin(TableReference toTable) {
    return copyOnWriteOrMutate(
        b -> b.innerJoin(toTable),
        () -> joins.add(new Join(JoinType.INNER_JOIN, toTable))
    );
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
   * @return a new select statement with the change applied.
   */
  public T innerJoin(SelectStatement subSelect, Criterion onCondition) {
    return copyOnWriteOrMutate(
        b -> b.innerJoin(subSelect, onCondition),
        () -> joins.add(new Join(JoinType.INNER_JOIN, subSelect, onCondition))
    );
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
   * @return a new select statement with the change applied.
   */
  public T crossJoin(SelectStatement subSelect) {
    return copyOnWriteOrMutate(
        b -> b.crossJoin(subSelect),
        () -> joins.add(new Join(JoinType.INNER_JOIN, subSelect, null))
    );
  }

  /**
   * @param subSelect the sub select statement to join on to
   * @return a new select statement with the change applied.
   *
   * @deprecated Use {@link #crossJoin(SelectStatement)} to do a cross join;
   *             or add join conditions for {@link #innerJoin(SelectStatement, Criterion)}
   *             to make this an inner join.
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  public T innerJoin(SelectStatement subSelect) {
    return copyOnWriteOrMutate(
        b -> b.innerJoin(subSelect),
        () -> joins.add(new Join(JoinType.INNER_JOIN, subSelect))
    );
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
   * @return a new select statement with the change applied.
   */
  public T leftOuterJoin(TableReference toTable, Criterion criterion) {
    return copyOnWriteOrMutate(
        b -> b.leftOuterJoin(toTable, criterion),
        () -> joins.add(new Join(JoinType.LEFT_OUTER_JOIN, toTable, criterion))
    );
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
   * @return a new select statement with the change applied.
   */
  public T leftOuterJoin(SelectStatement subSelect, Criterion criterion) {
    return copyOnWriteOrMutate(
        b -> b.leftOuterJoin(subSelect, criterion),
        () -> joins.add(new Join(JoinType.LEFT_OUTER_JOIN, subSelect, criterion))
    );
  }


  /**
   * Specifies a full outer join to a table:
   *
   * <blockquote><pre>
   * TableReference foo = tableRef("Foo");
   * TableReference bar = tableRef("Bar");
   * SelectStatement statement = select()
   *     .from(foo)
   *     .fullOuterJoin(bar, foo.field(&quot;id&quot;).eq(bar.field(&quot;fooId&quot;)));</pre>
   * </blockquote>
   *
   * @param toTable the table to join to
   * @param criterion the criteria on which to join the tables
   * @return a new select statement with the change applied.
   */
  public T fullOuterJoin(TableReference toTable, Criterion criterion) {
    return copyOnWriteOrMutate(
        b -> b.fullOuterJoin(toTable, criterion),
        () -> joins.add(new Join(JoinType.FULL_OUTER_JOIN, toTable, criterion))
    );
  }


  /**
   * Specifies an full outer join to a subselect:
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
   *     .fullOuterJoin(amountsByAgeLastMonth, amountsByAgeLastMonth.asTable().field("age").eq(customer.field("age")));
   * </pre></blockquote>
   *
   * @param subSelect the sub select statement to join on to
   * @param criterion the criteria on which to join the tables
   * @return a new select statement with the change applied.
   */
  public T fullOuterJoin(SelectStatement subSelect, Criterion criterion) {
    return copyOnWriteOrMutate(
        b -> b.fullOuterJoin(subSelect, criterion),
        () -> joins.add(new Join(JoinType.FULL_OUTER_JOIN, subSelect, criterion))
    );
  }


  /**
   * Specifies the where criteria:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from("Foo").where(field("name").eq("Dave"));
   * </pre></blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return a new select statement with the change applied.
   */
  public T where(Criterion criterion) {
    return copyOnWriteOrMutate(
        b -> b.where(criterion),
        () -> {
          if (criterion == null) {
            throw new IllegalArgumentException("Criterion was null in where clause");
          }
          whereCriterion = criterion;
        }
    );
  }


  /**
   * Specifies the where criteria. For use in code where the criteria are being generated dynamically.
   * The iterable can be empty but not null.
   *
   * @param criteria the criteria to filter the results by. They will be <i>AND</i>ed together.
   * @return a new select statement with the change applied.
   */
  public T where(Iterable<Criterion> criteria) {
    return copyOnWriteOrMutate(
        b -> b.where(criteria),
        () -> {
          if (criteria == null) {
            throw new IllegalArgumentException("No criterion was given in the where clause");
          }
          if (!Iterables.isEmpty(criteria)) {
            whereCriterion = new Criterion(Operator.AND, criteria);
          }
        }
    );
  }


  /**
   * Specifies the fields by which to order the result set:
   *
   * <blockquote><pre>
   * SelectStatement statement = select().from("Foo").orderBy(field("name").desc());
   * </pre></blockquote>
   *
   * @param orderFields the fields to order by
   * @return a new select statement with the change applied.
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
   * @return a new select statement with the change applied.
   */
  public T orderBy(Iterable<AliasedField> orderFields) {
    return copyOnWriteOrMutate(
        b -> b.orderBy(orderFields),
        () -> {
          if (orderFields == null) {
            throw new IllegalArgumentException("Fields were null in order by clause");
          }

          // Add the list
          Iterables.addAll(orderBys, orderFields);

          // Default fields to ascending if no direction has been specified
          SqlInternalUtils.defaultOrderByToAscending(orderBys);
        }
    );
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
      .dispatch(table)
      .dispatch(fields)
      .dispatch(joins)
      .dispatch(fromSelects)
      .dispatch(whereCriterion)
      .dispatch(orderBys);
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {

    StringBuilder result = new StringBuilder();
    if (fields.isEmpty()) {
      result.append("*");
    } else {
      result.append(fields);
    }

    if (table != null) {
      result.append(" FROM [").append(table).append("]");
    }

    if (!fromSelects.isEmpty()) {
      result.append(" FROM ").append(fromSelects);
    }

    if (!joins.isEmpty()) result.append(" ");
    result.append(StringUtils.join(joins, " "));

    if (whereCriterion != null) result.append(" WHERE [").append(whereCriterion).append("]");
    if (!orderBys.isEmpty()) result.append(" ORDER BY ").append(orderBys);
    return result.toString();
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (alias == null ? 0 : alias.hashCode());
    result = prime * result + (fields == null ? 0 : fields.hashCode());
    result = prime * result + (fromSelects == null ? 0 : fromSelects.hashCode());
    result = prime * result + (joins == null ? 0 : joins.hashCode());
    result = prime * result + (orderBys == null ? 0 : orderBys.hashCode());
    result = prime * result + (table == null ? 0 : table.hashCode());
    result = prime * result + (whereCriterion == null ? 0 : whereCriterion.hashCode());
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
    @SuppressWarnings("unchecked")
    AbstractSelectStatement<T> other = (AbstractSelectStatement<T>) obj;
    if (alias == null) {
      if (other.alias != null)
        return false;
    } else if (!alias.equals(other.alias))
      return false;
    if (fields == null) {
      if (other.fields != null)
        return false;
    } else if (!fields.equals(other.fields))
      return false;
    if (fromSelects == null) {
      if (other.fromSelects != null)
        return false;
    } else if (!fromSelects.equals(other.fromSelects))
      return false;
    if (joins == null) {
      if (other.joins != null)
        return false;
    } else if (!joins.equals(other.joins))
      return false;
    if (orderBys == null) {
      if (other.orderBys != null)
        return false;
    } else if (!orderBys.equals(other.orderBys))
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    if (whereCriterion == null) {
      if (other.whereCriterion != null)
        return false;
    } else if (!whereCriterion.equals(other.whereCriterion))
      return false;
    return true;
  }
}