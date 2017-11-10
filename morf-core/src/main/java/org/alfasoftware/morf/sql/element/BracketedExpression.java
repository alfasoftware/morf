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

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Represents Math expression wrapped by a bracket.
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class BracketedExpression extends AliasedField implements Driver {

  /**
   * Inner expression, that is wrapped by a bracket.
   */
  private final MathsField innerExpression;


  /**
   * Method that wraps a first elements of an (sub)expression with a bracket.
   * <p>
   * For example, in order to generate "(a + b) / c" SQL Math expression, we
   * need to put first two elements (first subexpression) into a bracket. That
   * could be achieved by the following DSL statement.
   * </p>
   *
   * <pre>
   * bracket(field(&quot;a&quot;).plus(field(&quot;b&quot;))).divideBy(field(&quot;c&quot;))
   * </pre>
   *
   *
   * @param expression the input Math expression that will be wrapped with
   *          brackets in output SQL
   * @return new expression containing the input expression wrapped with
   *         brackets
   */
  public static BracketedExpression bracket(MathsField expression) {
    return new BracketedExpression("", expression);
  }


  @Override
  public BracketedExpression as(String aliasName) {
    return (BracketedExpression) super.as(aliasName);
  }


  /**
   * @param innerExpression expression to be wrapped with a bracket.
   */
  public BracketedExpression(MathsField innerExpression) {
    super();
    this.innerExpression = innerExpression;
  }


  private BracketedExpression(final String alias, MathsField innerExpression) {
    super(alias);
    this.innerExpression = innerExpression;
  }


  /**
   * @see #innerExpression
   * @return The inner expression
   */
  public MathsField getInnerExpression() {
    return innerExpression;
  }


  @Override
  protected BracketedExpression deepCopyInternal(DeepCopyTransformation transformer) {
    return new BracketedExpression(this.getAlias(), (MathsField) transformer.deepCopy(innerExpression));
  }


  @Override
  protected BracketedExpression shallowCopy(String aliasName) {
    return new BracketedExpression(aliasName, innerExpression);
  }


  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getInnerExpression());
  }


  @Override
  public String toString() {
    return "(" + innerExpression + ")" + super.toString();
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((innerExpression == null) ? 0 : innerExpression.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    BracketedExpression other = (BracketedExpression) obj;
    if (innerExpression == null) {
      if (other.innerExpression != null)
        return false;
    } else if (!innerExpression.equals(other.innerExpression))
      return false;
    return true;
  }
}