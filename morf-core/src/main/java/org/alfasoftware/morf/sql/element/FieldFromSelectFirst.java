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

import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Creates a field from a {@link SelectFirstStatement}. The select statement should only have one field
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class FieldFromSelectFirst extends AliasedField implements Driver {

  /**
   * The select statement to get the field from
   */
  private final SelectFirstStatement selectFirstStatement;


  /**
   * Constructor used to create the deep copy of this field from select
   *
   * @param sourceField the field from select to copy from
   * @param transformer The transformation to execute during the copy
   */
  private FieldFromSelectFirst(FieldFromSelectFirst sourceField,DeepCopyTransformation transformer) {
    super();

    if (sourceField.getAlias() != null) {
      this.as(sourceField.getAlias());
    }

    this.selectFirstStatement = transformer.deepCopy(sourceField.selectFirstStatement);
  }

  /**
   * Constructor to create a field from a {@link SelectFirstStatement}
   *
   * @param selectStatement the {@literal SelectFirstStatement} to create the field
   *          from
   */
  public FieldFromSelectFirst(SelectFirstStatement selectStatement) {
    super();

    this.selectFirstStatement = selectStatement;
  }


  /**
   * @return the selectStatement
   */
  public SelectFirstStatement getSelectFirstStatement() {
    return selectFirstStatement;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new FieldFromSelectFirst(this,transformer);
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getSelectFirstStatement());
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return selectFirstStatement.toString() + super.toString();
  }
}