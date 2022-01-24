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

import java.util.List;

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Represents a case select statement.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class CaseStatement extends AliasedField implements Driver {

  /** When conditions */
  private final ImmutableList<WhenCondition> whenConditions;

  /** If all the when conditions fail return this default value*/
  private final AliasedField defaultValue;


  /**
   * Constructor.
   *
   * @param defaultValue  If all the when conditions fail return this default value
   * @param whenConditions when condition
   */
  public CaseStatement(AliasedField defaultValue, WhenCondition... whenConditions) {
    super();
    this.whenConditions = ImmutableList.copyOf(whenConditions);
    this.defaultValue = defaultValue;
  }


  private CaseStatement(String alias, AliasedField defaultValue, ImmutableList<WhenCondition> whenConditions) {
    super(alias);
    this.whenConditions = whenConditions;
    this.defaultValue = defaultValue;
  }


  /**
   * Constructor used for deep copying
   *
   * @param caseStatement case statement.
   * @param transformer The transformer operation to perform during the copy
   */
  private CaseStatement(CaseStatement caseStatement, DeepCopyTransformation transformer) {
    super(caseStatement.getAlias());
    this.whenConditions = FluentIterable.from(caseStatement.whenConditions)
        .transform(transformer::deepCopy)
        .toList();
    this.defaultValue = transformer.deepCopy(caseStatement.getDefaultValue());
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected CaseStatement deepCopyInternal(DeepCopyTransformation transformer) {
    return new CaseStatement(this,transformer);
  }


  @Override
  protected CaseStatement shallowCopy(String aliasName) {
    return new CaseStatement(aliasName, defaultValue, whenConditions);
  }


  /**
   * @return the whenConditions
   */
  public List<WhenCondition> getWhenConditions() {
    return whenConditions;
  }


  /**
   * @return the defaultValue
   */
  public AliasedField getDefaultValue() {
    return defaultValue;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getWhenConditions())
      .dispatch(getDefaultValue());
  }


  /**
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("CASE ");
    for (WhenCondition whenCondition : whenConditions) {
      result.append(whenCondition).append(" ");
    }
    result.append("ELSE ").append(defaultValue);
    result.append(" END");
    result.append(super.toString());
    return result.toString();
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (defaultValue == null ? 0 : defaultValue.hashCode());
    result = prime * result + (whenConditions == null ? 0 : whenConditions.hashCode());
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
    CaseStatement other = (CaseStatement) obj;
    if (defaultValue == null) {
      if (other.defaultValue != null)
        return false;
    } else if (!defaultValue.equals(other.defaultValue))
      return false;
    if (whenConditions == null) {
      if (other.whenConditions != null)
        return false;
    } else if (!whenConditions.equals(other.whenConditions))
      return false;
    return true;
  }


  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
    if(whenConditions != null) {
      whenConditions.stream().forEach(wc -> wc.accept(visitor));
    }
    if(defaultValue != null) {
      defaultValue.accept(visitor);
    }
  }
}
