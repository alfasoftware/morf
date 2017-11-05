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
 * This is an interface for handling SQL statements. This
 * is used as a common base for all SQL statements.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface Statement {


  /**
   * Create a deep copy for this statement
   *
   * @return a deep copy of this statement
   * @deprecated use deepCopy({@link DeepCopyTransformation}) which returns a builder.
   */
  @Deprecated
  public Statement deepCopy();

}
