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

import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Creates a field from a {@link SelectStatement}. The select statement should only have one field
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class FieldFromSelect extends AliasedField implements Driver{

  /**
   * The select statement to get the field from
   */
  private final SelectStatement selectStatement;


  /**
   * Constructor used to create the deep copy of this field from select
   *
   * @param sourceField the field from select to copy from
   */
  private FieldFromSelect(FieldFromSelect sourceField,DeepCopyTransformation transformer) {
    super();

    if (sourceField.getAlias() != null) {
      this.as(sourceField.getAlias());
    }

    this.selectStatement = transformer.deepCopy(sourceField.selectStatement);
  }

  /**
   * Constructor to create a field from a {@link SelectStatement}
   *
   * @param selectStatement the {@literal SelectStatement} to create the field
   *          from
   */
  public FieldFromSelect(SelectStatement selectStatement) {
    super();

    if (selectStatement.getFields().size() > 1) {
      throw new IllegalArgumentException("Select statement to create a field from select can not have more than one field");
    }

    this.selectStatement = selectStatement;
  }


  /**
   * @return the selectStatement
   */
  public SelectStatement getSelectStatement() {
    return selectStatement;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new FieldFromSelect(this,transformer);
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getSelectStatement());
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return selectStatement.toString() + super.toString();
  }
}