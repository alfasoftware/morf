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

package org.alfasoftware.morf.sql.element;

import static org.alfasoftware.morf.util.DeepCopyTransformations.noTransformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.TempTransitionalBuilderWrapper;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

import com.google.common.collect.Iterables;

/**
 * A representation of a single criterion for use by conditional clauses in an
 * SQL statement.
 *
 * <p>This class models an operator and one or two operands. In addition, it
 * provides a number of static helper methods which will create criteria that
 * perform common operations.</p>
 *
 * <p>In general, objects of this class should be instantiated throw the
 * helper methods. For example:</p>
 *
 * <blockquote><pre>
 *    Criterion.eq(new Field("agreementnumber"), "A0001");</pre></blockquote>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class Criterion  implements Driver,DeepCopyableWithTransformation<Criterion,Builder<Criterion>>{

  /**
   * Operator to use in the criterion
   */
  private final Operator operator;

  /**
   * Select statement to use
   * as part of unary statement (e.g. IN)
   */
  private SelectStatement selectStatement;

  /**
   * The left hand side field
   */
  private AliasedField field;

  /**
   * The right hand side field or literal
   */
  private Object value;

  /**
   * The additional sub-criteria
   */
  private final List<Criterion> criteria = new ArrayList<>();


  /**
   * Constructor used to create deep copy of a criterion
   *
   * @param sourceCriterion the source criterion to create the deep copy from
   */
  private Criterion(Criterion sourceCriterion,DeepCopyTransformation transformer) {
    super();

    this.operator = sourceCriterion.operator;

    this.selectStatement = transformer.deepCopy(sourceCriterion.selectStatement);
    this.field = transformer.deepCopy(sourceCriterion.field);

    // Copy the child criteria
    for (Criterion currentCriterion : sourceCriterion.criteria) {
      this.criteria.add(transformer.deepCopy(currentCriterion));
    }

    // Aliased Fields can be copied, otherwise we just use the same reference
    if (sourceCriterion.value instanceof AliasedField) {
      this.value = transformer.deepCopy((AliasedField)sourceCriterion.value);
    } else {
      this.value = sourceCriterion.value;
    }
  }

  /**
   * Construct a new criterion with an operator and series of other criteria.
   * This is commonly used for the "AND" and "OR" operators.
   *
   * @param operator the operator to use in the criterion
   * @param criteria the criteria
   */
  public Criterion(Operator operator, Iterable<Criterion> criteria) {
    if (criteria == null || Iterables.isEmpty(criteria)) {
      throw new IllegalArgumentException("Must specify at least one criterion");
    }

    this.operator = operator;
    Iterables.addAll(this.criteria, criteria);
  }

  /**
   * Construct a new criterion with an operator and series of other criteria.
   * This is commonly used for the "AND" and "OR" operators.
   *
   * @param operator the operator to use in the criterion
   * @param criterion the first criterion in this criterion
   * @param criteria subsequent criteria
   */
  public Criterion(Operator operator, Criterion criterion, Criterion...criteria) {
    if (criterion == null && operator != Operator.EXISTS)
      throw new IllegalArgumentException("Must specify at least one criterion");

    this.operator = operator;
    this.criteria.add(criterion);
    this.criteria.addAll(Arrays.asList(criteria));
  }

  /**
   * Construct a new criterion based on a unary operator and a {@link SelectStatement}.
   * This is commonly used for the "EXISTS" operator.
   *
   * @param operator the unary operator to use in the criterion
   * @param selectStatement the select statement associated with the operator
   */
  public Criterion(Operator operator, SelectStatement selectStatement) {
    this.operator = operator;
    this.selectStatement = selectStatement;
  }


  /**
   * Construct a new criterion based on a binary operator where the left-hand
   * operand is a field and the right-hand operand is a {@link SelectStatement}.
   * This is commonly used for the "IN" operator.
   *
   * @param operator the binary operator to use in the criterion
   * @param field the left-hand operand field
   * @param selectStatement the select statement association with the operator
   *          and applied to the field
   */
  public Criterion(Operator operator, AliasedField field, SelectStatement selectStatement) {
    // Make sure that the following is now allowed:
    // ... WHERE fieldA IN (SELECT fieldA, fieldB FROM ...)
    // ... WHERE fieldA IN (SELECT * FROM ...)
    if (operator == Operator.IN && selectStatement.getFields().size() != 1) {
      throw new IllegalArgumentException("Subquery can only contain 1 column");
    }
    this.operator = operator;
    this.field = field;
    this.selectStatement = selectStatement;
  }


  /**
   * Construct a new criterion based on a binary operator which evaluates a field and some
   * other value. The other value may be a field itself or a constant.
   *
   * @param operator the binary operator to use in the criterion
   * @param field the field to evaluate (the left hand side of the expression)
   * @param value the value to evaluate (the right hand side of the expression)
   */
  public Criterion(Operator operator, AliasedField field, Object value) {
    if (field == null)
      throw new IllegalArgumentException("Field cannot be null in a binary criterion");

    this.operator = operator;
    this.field = field;
    this.value = value;
  }

  /**
   * Helper method to create a new "AND" expression.
   *
   * <blockquote><pre>
   *    Criterion.and(Criterion.eq(new Field("agreementnumber"), "A0001"), Criterion.greaterThan(new Field("startdate"), 20090101));</pre></blockquote>
   *
   * @param criterion the first in the list of criteria
   * @param criteria the remaining criteria
   * @return a new Criterion object
   */
  public static Criterion and(Criterion criterion, Criterion... criteria) {
    return new Criterion(Operator.AND, criterion, criteria);
  }

  /**
   * Helper method to create a new "OR" expression.
   *
   * <blockquote><pre>
   *    Criterion.or(Criterion.eq(new Field("agreementnumber"), "A0001"), Criterion.greaterThan(new Field("startdate"), 20090101));</pre></blockquote>
   *
   * @param criterion the first in the list of criteria
   * @param criteria the remaining criteria
   * @return a new Criterion object
   */
  public static Criterion or(Criterion criterion, Criterion... criteria) {
    return new Criterion(Operator.OR, criterion, criteria);
  }

  /**
   * Helper method to create a new "NOT" expression.
   *
   * <blockquote><pre>
   *    Criterion.not(Criterion.eq(new Field("agreementnumber"), "A0001"));</pre></blockquote>
   *
   * @param criterion the first in the list of criteria
   * @return a new Criterion object
   */
  public static Criterion not(Criterion criterion) {
    return new Criterion(Operator.NOT, criterion);
  }

  /**
   * Helper method to create a new "EXISTS" expression.
   *
   * <blockquote><pre>
   *    SelectStatement stmt = new SelectStatement(new Table("agreement")).where(Criterion.eq(new Field("agreementnumber"), "A0001"));
   *    Criterion.exists(stmt);</pre></blockquote>
   *
   * @param selectStatement the select statement to evaluate
   * @return a new Criterion object
   */
  public static Criterion exists(SelectStatement selectStatement) {
    return new Criterion(Operator.EXISTS, selectStatement);
  }


  /**
   * Helper method to create a new "IN" expression.
   *
   * <blockquote>
   * <pre>
   * SelectStatement stmt = select()
   *   .from(tableRef("Schedule"))
   *   .where(
   *     Criterion.in(field("chargeType"), select(field("chargeType").from(somewhere)))
   *   )
   * </pre>
   * </blockquote>
   *
   * <strong>Any null values returned by {@code selectStatement}
   * that are compared to {@code field} can produce unexpected
   * results.</strong>
   *
   * @param field the field to evaluate (the left-hand side of the expression)
   * @param selectStatement the select statement to evaluate (the right-hand side
   *          of the expression)
   * @return a new Criterion object
   */
  public static Criterion in(AliasedField field, SelectStatement selectStatement) {
    return new Criterion(Operator.IN, field, selectStatement);
  }


  /**
   * Helper method to create a new "IN" expression.
   *
   * <blockquote>
   * <pre>
   * SelectStatement stmt = select()
   *   .from(tableRef("Schedule"))
   *   .where(
   *     Criterion.in(field("chargeType"), 1, 2, 3)
   *   )
   * </pre>
   * </blockquote>
   *
   * <strong>Any null values returned by {@code selectStatement}
   * that are compared to {@code field} can produce unexpected
   * results.</strong>
   *
   * @param field the field to evaluate (the left-hand side of the expression)
   * @param values the list of values (the right-hand side of the expression)
   * @return a new Criterion object
   */
  public static Criterion in(AliasedField field, Object... values) {
    return new Criterion(Operator.IN, field, Arrays.asList(values));
  }


  /**
   * Helper method to create a new "EQUALS" expression.
   *
   * <blockquote><pre>
   *    Criterion.eq(new Field("agreementnumber"), "A0001");</pre></blockquote>
   *
   * @param field the field to evaluate in the expression (the left hand side of the expression)
   * @param value the value to evaluate in the expression (the right hand side)
   * @return a new Criterion object
   */
  public static Criterion eq(AliasedField field, Object value) {
    return new Criterion(Operator.EQ, field, value);
  }

  /**
   * Helper method to create a new "NOT EQUAL" expression.
   *
   * <blockquote><pre>
   *    Criterion.neq(new Field("agreementnumber"), "A0001");</pre></blockquote>
   *
   * @param field the field to evaluate in the expression (the left hand side of the expression)
   * @param value the value to evaluate in the expression (the right hand side)
   * @return a new Criterion object
   */
  public static Criterion neq(AliasedField field, Object value) {
    return new Criterion(Operator.NEQ, field, value);
  }

  /**
   * Helper method to create a new "LESS THAN" expression.
   *
   * <blockquote><pre>
   *    Criterion.lessThan(new Field("startdate"), 20091001);</pre></blockquote>
   *
   * @param field the field to evaluate in the expression (the left hand side of the expression)
   * @param value the value to evaluate in the expression (the right hand side)
   * @return a new Criterion object
   */
  public static Criterion lessThan(AliasedField field, Object value) {
    return new Criterion(Operator.LT, field, value);
  }

  /**
   * Helper method to create a new "GREATER THAN" expression.
   *
   * <blockquote><pre>
   *    Criterion.greaterThan(new Field("startdate"), 20091001);</pre></blockquote>
   *
   * @param field the field to evaluate in the expression (the left hand side of the expression)
   * @param value the value to evaluate in the expression (the right hand side)
   * @return a new Criterion object
   */
  public static Criterion greaterThan(AliasedField field, Object value) {
    return new Criterion(Operator.GT, field, value);
  }

  /**
   * Helper method to create a new "LESS THAN OR EQUAL TO" expression.
   *
   * <blockquote><pre>
   *    Criterion.lessThanOrEqualTo(new Field("startdate"), 20091001);</pre></blockquote>
   *
   * @param field the field to evaluate in the expression (the left hand side of the expression)
   * @param value the value to evaluate in the expression (the right hand side)
   * @return a new Criterion object
   */
  public static Criterion lessThanOrEqualTo(AliasedField field, Object value) {
    return new Criterion(Operator.LTE, field, value);
  }

  /**
   * Helper method to create a new "GREATER THAN OR EQUAL TO" expression.
   *
   * <blockquote><pre>
   *    Criterion.greaterThanOrEqualTo(new Field("startdate"), 20091001);</pre></blockquote>
   *
   * @param field the field to evaluate in the expression (the left hand side of the expression)
   * @param value the value to evaluate in the expression (the right hand side)
   * @return a new Criterion object
   */
  public static Criterion greaterThanOrEqualTo(AliasedField field, Object value) {
    return new Criterion(Operator.GTE, field, value);
  }

  /**
   * Helper method to create a new "IS NULL" expression.
   *
   * <blockquote><pre>
   *    Criterion.isNull(new Field("agreementflag"));</pre></blockquote>
   *
   * @param field the field to check if null
   * @return a new Criterion object
   */
  public static Criterion isNull(AliasedField field) {
    return new Criterion(Operator.ISNULL, field, null);
  }

  /**
   * Helper method to create a new "IS NOT NULL" expression.
   *
   * <blockquote><pre>
   *    Criterion.isNotNull(new Field("agreementflag"));</pre></blockquote>
   *
   * @param field the field to check if not null
   * @return a new Criterion object
   */
  public static Criterion isNotNull(AliasedField field) {
    return new Criterion(Operator.ISNOTNULL, field, null);
  }

  /**
   * Helper method to create a new "LIKE" expression.
   *
   * <blockquote><pre>
   *    Criterion.like(new Field("agreementnumber"), "A%");</pre></blockquote>
   *
   * <p>Note the escape character is set to '\' (backslash) by the underlying system.</p>
   *
   * @param field the field to evaluate in the expression (the left hand side of the expression)
   * @param value the value to evaluate in the expression (the right hand side)
   * @return a new Criterion object
   */
  public static Criterion like(AliasedField field, Object value) {
    return new Criterion(Operator.LIKE, field, value);
  }

  /**
   * Get the operator associated with the criterion
   *
   * @return the operator
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * Get the list of criteria (if any) associated with the criterion
   *
   * @return the criteria
   */
  public List<Criterion> getCriteria() {
    return criteria;
  }

  /**
   * Get the {@link SelectStatement} associated with the criterion.
   *
   * @return the selectStatement
   */
  public SelectStatement getSelectStatement() {
    return selectStatement;
  }

  /**
   * Get the {@link AliasedField} associated with the criterion. This is the left hand
   * side of an expression.
   *
   * @return the field
   */
  public AliasedField getField() {
    return field;
  }


  /**
   * Get the value associate with the criterion. This is the right hand side
   * of an expression.
   *
   * @return the value
   */
  public Object getValue() {
    return value;
  }


  /**
   * Create a deep copy for this criteria
   *
   * @return a deep copy of this criteria
   */
  public Criterion deepCopy() {
    return new Criterion(this,noTransformation());
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    if (selectStatement != null) {
      return operator.toString() + " " + selectStatement;
    }
    if (criteria.isEmpty()) {
      return String.format("%s %s %s", field, operator, value);
    }
    StringBuilder result = new StringBuilder();
    boolean first = true;
    for (Criterion criterion : criteria) {
      if (!first)result.append(" ").append(operator).append(" ");
      result.append("(").append(criterion).append(")");
      first =  false;
    }
    return result.toString();
  }



  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getField());
    visitValue(traverser,getValue());
    traverser
      .dispatch(getSelectStatement())
      .dispatch(getCriteria());
  }


  @SuppressWarnings("rawtypes")
  private void visitValue(ObjectTreeTraverser traverser,Object value) {
    if (value instanceof AliasedField) {
      traverser.dispatch( (AliasedField)value);
    } else if (value instanceof Iterable) {
      for (Object child : (Iterable)value) {
        visitValue(traverser,child);
      }
    }
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<Criterion> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new Criterion(this,transformer));
  }
}