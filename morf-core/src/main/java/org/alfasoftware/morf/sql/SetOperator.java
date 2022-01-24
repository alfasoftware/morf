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

import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Provides a common interface for all set operators which can be applied to
 * SELECT statements, such as UNION and MINUS.
 * <p>
 * The {@link SetOperator} implementations <em>should not</em> provide
 * {@code public} constructors as they should be instantiated directly by
 * {@linkplain SelectStatement} set operations, such as
 * {@linkplain SelectStatement#union(SelectStatement)}.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public interface SetOperator extends Driver, DeepCopyableWithTransformation<SetOperator, Builder<SetOperator>>, SchemaAndDataChangeVisitable {

  /**
   * @return The right-hand operand this operation is being performed against.
   */
  public SelectStatement getSelectStatement();
}
