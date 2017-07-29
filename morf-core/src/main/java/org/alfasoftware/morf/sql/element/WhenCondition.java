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

import org.alfasoftware.morf.sql.TempTransitionalBuilderWrapper;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;


/**
 * Represent the when condition in a case select.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class WhenCondition implements Driver,DeepCopyableWithTransformation<WhenCondition,Builder<WhenCondition>>{

  /** Value */
  private final AliasedField value;

  /** Criterion*/
  private final Criterion criterion;

  /**
   * Constructor used for deep copying.
   *
   * @param whenCondition When condition
   */
  private WhenCondition(WhenCondition whenCondition,DeepCopyTransformation transformer) {
    this.value = transformer.deepCopy(whenCondition.getValue());
    this.criterion = transformer.deepCopy(whenCondition.getCriterion());
  }


  /**
   * Constructor.
   *
   * @param criterion Criteria
   * @param value The value returned if the criteria is true
   */
  public WhenCondition(Criterion criterion, AliasedField value) {
    super();
    this.value = value;
    this.criterion = criterion;
  }


  /**
   * @return the value
   */
  public AliasedField getValue() {
    return value;
  }


  /**
   * @return the criterion
   */
  public Criterion getCriterion() {
    return criterion;
  }


  /**
   * Creates a deep copy of {@link WhenCondition}
   *
   * @return deep copy of the field
   */
  public WhenCondition deepCopy() {
    return new WhenCondition(this,DeepCopyTransformations.noTransformation());
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getCriterion())
      .dispatch(getValue());
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "WHEN [" + criterion + "] THEN [" + value + "]";
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */

  @Override
  public Builder<WhenCondition> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new WhenCondition(this,transformer));
  }
}
