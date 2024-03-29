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

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Representation of a CAST function.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class Cast extends AliasedField implements Driver {

  /**
   * The field to cast from.
   */
  private final AliasedField expression;

  /**
   * The width of the data type to cast to.
   */
  private final int width;

  /**
   * The scale of the data type to cast to.
   */
  private final int scale;

  /**
   * The data type to cast to.
   */
  private final DataType dataType;


  /**
   * Constructor.
   *
   * @param expression the expression to cast.
   * @param dataType the data type to cast to.
   * @param width the width.
   * @param scale the scale.
   */
  public Cast(AliasedField expression, DataType dataType, int width, int scale) {
    super();
    this.expression = expression;
    this.dataType = dataType;
    this.width = width;
    this.scale = scale;
  }


  private Cast(String alias, AliasedField expression, DataType dataType, int width, int scale) {
    super(alias);
    this.expression = expression;
    this.dataType = dataType;
    this.width = width;
    this.scale = scale;
  }


  /**
   * Constructor.
   *
   * @param expression the expression to cast.
   * @param dataType the data type to cast to.
   * @param length the length.
   */
  public Cast(AliasedField expression, DataType dataType, int length) {
    this(expression, dataType, length, 0);
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new Cast(getAlias(), transformer.deepCopy(expression), dataType, width, scale);
  }


  @Override
  protected AliasedField shallowCopy(String aliasName) {
    return new Cast(aliasName, expression, dataType, width, scale);
  }


  /**
   * @return The {@link AliasedField} being casted
   */
  public AliasedField getExpression() {
    return expression;
  }


  /**
   * @return the dataType
   */
  public DataType getDataType() {
    return dataType;
  }


  /**
   * @return the width
   */
  public int getWidth() {
    return width;
  }


  /**
   * @return the scale
   */
  public int getScale() {
    return scale;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#as(java.lang.String)
   */
  @Override
  public Cast as(String aliasName) {
    return (Cast) super.as(aliasName);
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getExpression());
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return String.format("CAST(%s AS %s(%s, %s))%s", expression, dataType, width, scale, super.toString());
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (dataType == null ? 0 : dataType.hashCode());
    result = prime * result + (expression == null ? 0 : expression.hashCode());
    result = prime * result + scale;
    result = prime * result + width;
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
    Cast other = (Cast) obj;
    if (dataType != other.dataType)
      return false;
    if (expression == null) {
      if (other.expression != null)
        return false;
    } else if (!expression.equals(other.expression))
      return false;
    if (scale != other.scale)
      return false;
    if (width != other.width)
      return false;
    return true;
  }


  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
    if(expression != null) {
      expression.accept(visitor);
    }
  }
}