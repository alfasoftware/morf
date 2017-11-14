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
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.util.DeepCopyTransformation;

/**
 * Provides a representation of a null literal field value to be used in a {@link Statement}.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class NullFieldLiteral extends FieldLiteral {

  public NullFieldLiteral() {
    super();
  }


  NullFieldLiteral(String alias) {
    super(alias, null, DataType.NULL);
  }

  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected NullFieldLiteral deepCopyInternal(final DeepCopyTransformation transformer) {
    return new NullFieldLiteral(this.getAlias());
  }
}