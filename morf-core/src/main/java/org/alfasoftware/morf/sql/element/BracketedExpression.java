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
   * @param innerExpression expression to be wrapped with a bracket.
   */
  public BracketedExpression(MathsField innerExpression) {
    super();
    this.innerExpression = innerExpression;
  }


  /**
   * @see #innerExpression
   * @return The inner expression
   */
  public MathsField getInnerExpression() {
    return innerExpression;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new BracketedExpression((MathsField) transformer.deepCopy(innerExpression));
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getInnerExpression());
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "(" + innerExpression + ")" + super.toString();
  }
}