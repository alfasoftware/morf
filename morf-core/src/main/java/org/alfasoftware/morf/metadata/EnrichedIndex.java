/* Copyright 2026 Alfa Financial Software
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

package org.alfasoftware.morf.metadata;

import java.util.List;

/**
 * Decorator over an {@link Index} that carries additional metadata from
 * the DeployedIndexes table: whether the index is deferred and whether
 * it physically exists in the database catalog.
 *
 * <p>Created by the model enricher during schema building. The visitor
 * uses these properties to make DDL decisions without runtime IF EXISTS
 * checks.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class EnrichedIndex implements Index {

  private final Index delegate;
  private final boolean deferred;
  private final boolean physicallyPresent;


  /**
   * Creates an enriched index.
   *
   * @param delegate the underlying index.
   * @param deferred whether the index is deferred.
   * @param physicallyPresent whether the index physically exists in the DB.
   */
  public EnrichedIndex(Index delegate, boolean deferred, boolean physicallyPresent) {
    this.delegate = delegate;
    this.deferred = deferred;
    this.physicallyPresent = physicallyPresent;
  }


  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public List<String> columnNames() {
    return delegate.columnNames();
  }

  @Override
  public boolean isUnique() {
    return delegate.isUnique();
  }

  @Override
  public boolean isDeferred() {
    return deferred;
  }

  @Override
  public boolean isPhysicallyPresent() {
    return physicallyPresent;
  }

  @Override
  public String toString() {
    return toStringHelper();
  }
}
