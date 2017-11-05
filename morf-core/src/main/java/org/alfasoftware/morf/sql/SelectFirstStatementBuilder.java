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

package org.alfasoftware.morf.sql;

import org.alfasoftware.morf.util.DeepCopyTransformation;

/**
 * Builder for {@link SelectFirstStatement}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class SelectFirstStatementBuilder extends AbstractSelectStatementBuilder<SelectFirstStatement, SelectFirstStatementBuilder> {

  SelectFirstStatementBuilder() {
    super();
  }

  /**
   * Shallow copy constructor.
   *
   * @param copyOf The statement to copy.
   */
  SelectFirstStatementBuilder(SelectFirstStatement copyOf) {
    super(copyOf);
  }

  /**
   * Deep copy constructor.
   *
   * @param copyOf The statement to copy.
   * @param transformation The transformation to apply.
   */
  SelectFirstStatementBuilder(SelectFirstStatement copyOf, DeepCopyTransformation transformation) {
    super(copyOf, transformation);
  }

  @Override
  public SelectFirstStatement build() {
    return new SelectFirstStatement(this);
  }
}