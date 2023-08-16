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

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Represents a Maths operation.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class MathsField extends AliasedField implements Driver {

  /**
   * The left field.
   */
  private final AliasedField  leftField;

  /**
   * The right field.
   */
  private final AliasedField  rightField;

  /**
   * The operator.
   */
  private final MathsOperator operator;


  /**
   * Constructor.
   *
   * @param leftField the left part of the SQL.
   * @param operator the operator.
   * @param rightField the right part of the SQL.
   */
  public MathsField(AliasedField leftField, MathsOperator operator, AliasedField rightField) {
    super();
    this.leftField = leftField;
    this.operator = operator;
    this.rightField = rightField;
  }


  private MathsField(String alias, AliasedField leftField, MathsOperator operator, AliasedField rightField) {
    super(alias);
    this.leftField = leftField;
    this.operator = operator;
    this.rightField = rightField;
  }


  /**
   * @return the leftField
   */
  public AliasedField getLeftField() {
    return leftField;
  }


  /**
   * @return the rightField
   */
  public AliasedField getRightField() {
    return rightField;
  }


  /**
   * @return the operator
   */
  public MathsOperator getOperator() {
    return operator;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new MathsField(this.getAlias(), transformer.deepCopy(leftField), operator, transformer.deepCopy(rightField));
  }


  @Override
  protected AliasedField shallowCopy(String aliasName) {
    return new MathsField(aliasName, leftField, operator, rightField);
  }


  /**
   * Provides the plus operation for SQL.
   * @param leftField left addendum
   * @param rightField right addendum
   * @return The function representing the sum
   */
  public static MathsField plus(AliasedField leftField, AliasedField rightField ) {
    AliasedField rightOperand = rightField instanceof MathsField ? bracket((MathsField)rightField) : rightField;
    return new MathsField(leftField, MathsOperator.PLUS, rightOperand);
  }


  /**
   * Provides the multiply operation for SQL.
   * @param leftField left multiplier
   * @param rightField right multiplier
   * @return The function representing the product
   */
  public static MathsField multiply(AliasedField leftField, AliasedField rightField ) {
    AliasedField rightOperand = rightField instanceof MathsField ? bracket((MathsField)rightField) : rightField;
    return new MathsField(leftField, MathsOperator.MULTIPLY, rightOperand);
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getLeftField())
      .dispatch(getRightField());
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (leftField == null ? 0 : leftField.hashCode());
    result = prime * result + (operator == null ? 0 : operator.hashCode());
    result = prime * result + (rightField == null ? 0 : rightField.hashCode());
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
    MathsField other = (MathsField) obj;
    if (leftField == null) {
      if (other.leftField != null)
        return false;
    } else if (!leftField.equals(other.leftField))
      return false;
    if (operator != other.operator)
      return false;
    if (rightField == null) {
      if (other.rightField != null)
        return false;
    } else if (!rightField.equals(other.rightField))
      return false;
    return true;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return String.format("%s %s %s%s", leftField, operator, rightField, super.toString());
  }


  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
    if(leftField != null) {
      leftField.accept(visitor);
    }
    if(rightField != null) {
      rightField.accept(visitor);
    }
  }
}