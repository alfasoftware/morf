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

import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.apache.commons.lang.StringUtils;

/**
 * Provides a representation of a null literal field value to be used in a {@link Statement}.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class NullFieldLiteral extends FieldLiteral {

  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new NullFieldLiteral();
  }


  @Override
  public String toString() {
    return "NULL" + (StringUtils.isEmpty(getAlias()) ? "" : " AS " + getAlias());
  }
}
