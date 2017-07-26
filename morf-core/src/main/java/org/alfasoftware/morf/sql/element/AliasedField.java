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

import static org.alfasoftware.morf.sql.SqlUtils.bracket;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.util.DeepCopyTransformations.noTransformation;

import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.apache.commons.lang.StringUtils;

/**
 * An abstract base class common to all fields, functions
 * and literals used within SQL statements.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public abstract class AliasedField implements AliasedFieldBuilder, DeepCopyableWithTransformation<AliasedField,AliasedFieldBuilder>//  TEMPORARY - Transitional implementation of AliasedFieldBuilder
{

  /**
   * The alias to use for the field
   */
  private String alias = "";


  /**
   * deprecated use AliasedField(String)
   */
  @Deprecated
  protected AliasedField() {}


  protected AliasedField(String alias) {
    this.alias = alias;
  }


  /**
   * Specifies the alias to use for the field.
   *
   * @param aliasName the name of the alias
   * @return an updated {@link AliasedField} (this will not be a new object)
   */
  @Override
  public AliasedField as(String aliasName) {
    this.alias = aliasName;
    return this;
  }


  /**
    *<p>We are transitioning from a model where {@link AliasedField} and its descendants
   * are mutable - effectively combinations of the end object and its builder.  Some
   * {@link AliasedField} descendants have been converted to builders and immutable
   * instances, and some not.  In order for these to be used interchangeably,
   * we ensure that {@link AliasedField} can be treated as an {@link AliasedFieldBuilder},
   * returning itself.</p>
   *
   * @see org.alfasoftware.morf.sql.element.AliasedFieldBuilder#build()
   */
  @Override
  public final AliasedField build() {
    return this;
  }


  /**
   * Gets the alias of the field.
   *
   * @return the alias
   */
  public String getAlias() {
    return alias;
  }


  /**
   * Creates a deep copy of an aliased field
   *
   * @return deep copy of the field
   */
  public final AliasedField deepCopy() {
    return deepCopy(noTransformation()).build();
  }


  /**
   * Creates a deep copy of a descendant of {@link AliasedField},
   * populating properties in the descendant class
   * @param transformer the transformation to execute during the copy
   * @return deep copy of the field
   */
  protected abstract AliasedFieldBuilder deepCopyInternal(DeepCopyTransformation transformer);


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public AliasedFieldBuilder deepCopy(DeepCopyTransformation transformer) {
    AliasedFieldBuilder builder =  deepCopyInternal(transformer);

    if(!StringUtils.isBlank(this.alias)) {
      builder.as(this.alias);
    }

    return builder;
  }


  /**
   * Returns the name of the field either implied by its source or by its alias.
   * @return The implied name of the field
   */
  public String getImpliedName() {
    return getAlias();
  }


  /**
   * @return criteria for this field being null
   */
  public Criterion isNull() {
    return Criterion.isNull(this);
  }


  /**
   * @return The value, negated, with the original implied name.
   */
  public AliasedField negated() {
    return literal(0).minus(this).as(getImpliedName());
  }


  /**
   * @return criteria for this field being not null
   */
  public Criterion isNotNull() {
    return Criterion.isNotNull(this);
  }


  /**
   * @param value value to compare to
   * @return criteria for equality of this field to value.
   */
  public Criterion eq(Object value) {
    return Criterion.eq(this, value);
  }


  /**
   * @param value value to compare to
   * @return criteria for non-equality of this field to value.
   */
  public Criterion neq(Object value) {
    return Criterion.neq(this, value);
  }


  /**
   * @return this field cast from a YYYYMMDD string to a SQL date.
   */
  public AliasedField asDate() {
    return Function.yyyymmddToDate(this);
  }


  /**
   * @param value object to compare to (right hand side)
   * @return a {@link Criterion} for a less than or equal to expression of this field.
   */
  public Criterion lessThanOrEqualTo(Object value) {
    return Criterion.lessThanOrEqualTo(this, value);
  }


  /**
   * @param value object to compare to (right hand side)
   * @return a {@link Criterion} for a less than expression of this field.
   */
  public Criterion lessThan(Object value) {
    return Criterion.lessThan(this, value);
  }


  /**
   * @param value object to compare to (right hand side)
   * @return a {@link Criterion} for a greater than or equal to expression of this field.
   */
  public Criterion greaterThanOrEqualTo(Object value) {
    return Criterion.greaterThanOrEqualTo(this, value);
  }


  /**
   * @param value object to compare to (right hand side)
   * @return a {@link Criterion} for a greater than expression of this field.
   */
  public Criterion greaterThan(Object value) {
    return Criterion.greaterThan(this, value);
  }


  /**
   * @param expression value to add to this field.
   * @return A new expression using {@link MathsField} and {@link MathsOperator#PLUS}.
   */
  public final MathsField plus(AliasedField expression) {
    return new MathsField(this, MathsOperator.PLUS, potentiallyBracketExpression(expression));
  }


  /**
   * @param expression value to subtract from this field.
   * @return A new expression using {@link MathsField} and {@link MathsOperator#MINUS}.
   */
  public final MathsField minus(AliasedField expression) {
    return new MathsField(this, MathsOperator.MINUS, potentiallyBracketExpression(expression));
  }


  /**
   * @param expression value to multiply this field by.
   * @return A new expression using {@link MathsField} and {@link MathsOperator#MULTIPLY}.
   */
  public final MathsField multiplyBy(AliasedField expression) {
    return new MathsField(this, MathsOperator.MULTIPLY, potentiallyBracketExpression(expression));
  }


  /**
   * @param expression value to use as the denominator.
   * @return A new expression using {@link MathsField} and {@link MathsOperator#DIVIDE}.
   */
  public final MathsField divideBy(AliasedField expression) {
    return new MathsField(this, MathsOperator.DIVIDE, potentiallyBracketExpression(expression));
  }


  /**
   * Brackets the expression if the expression is a Math field. Only the nested
   * Math expressions should be put into the bracket during the expression
   * serialisation.
   * <p>
   * Expressions are represented by a binary-tree structure. This structure
   * holds information about all expression's operands. Each node is either a
   * terminating node (a leaf represented by a field or literal) or an Math
   * operation that has two operands.
   * </p>
   * <p>
   * A chain of Math operations can be grouped in sub-expressions using
   * BracketExpressions. That allows us to create an output SQL expressions that
   * contains brackets.
   * </p>
   */
  private AliasedField potentiallyBracketExpression(AliasedField expression) {
    return expression instanceof MathsField ? bracket((MathsField)expression) : expression;
  }


  /**
   * @param selectStatement Select statement to use as the right hand side of this criterion.
   * @return A {@link Criterion#in(AliasedField, org.alfasoftware.morf.sql.SelectStatement)}.
   */
  public Criterion in(SelectStatement selectStatement) {
    return Criterion.in(this, selectStatement);
  }


  /**
   * @param values values for comparison
   * @return A {@link Criterion#in(AliasedField, Object...)}.
   */
  public Criterion in(Object... values) {
    return Criterion.in(this, values);
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return StringUtils.isEmpty(alias) ? "" : " AS " + alias;
  }
}