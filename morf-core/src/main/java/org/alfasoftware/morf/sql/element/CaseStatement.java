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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Represents a case select statement.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class CaseStatement extends AliasedField implements Driver {

  /** When conditions */
  private List<WhenCondition> whenConditions = new ArrayList<>();

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
    this.whenConditions = Arrays.asList(whenConditions);
    this.defaultValue = defaultValue;
  }


  /**
   * Constructor used for deep copying
   *
   * @param caseStatement case statement.
   * @param transformer The transformer operation to perform during the copy
   */
  private CaseStatement(CaseStatement caseStatement,DeepCopyTransformation transformer) {
    super();
    for (WhenCondition whenCondition : caseStatement.getWhenConditions()) {
      whenConditions.add(transformer.deepCopy(whenCondition));
    }

    defaultValue = transformer.deepCopy(caseStatement.getDefaultValue());
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new CaseStatement(this,transformer);
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
    result.append(" END");
    result.append(super.toString());
    return result.toString();
  }
}
