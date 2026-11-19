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

package org.alfasoftware.morf.upgrade.deferredindexes;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Default implementation of {@link DeferredIndexService}. Reads non-{@code
 * COMPLETED} rows from {@link DeferredIndexesDAO} and wraps each in a
 * {@link DeferredIndexBuildTaskImpl} bound to the shared builder; progress
 * reads delegate straight to the DAO.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexServiceImpl implements DeferredIndexService {

  private final DeferredIndexBuilder builder;
  private final DeferredIndexesDAO dao;


  /**
   * @param builder shared reconciliation algorithm; bound to every fan-out task
   *     returned by {@link #getBuildTasks()}.
   * @param dao persistence layer for the DeferredIndexes table.
   */
  @Inject
  DeferredIndexServiceImpl(DeferredIndexBuilder builder, DeferredIndexesDAO dao) {
    this.builder = builder;
    this.dao = dao;
  }


  @Override
  public List<DeferredIndexBuildTask> getBuildTasks() {
    return dao.findNonTerminal().stream()
        .map(row -> DeferredIndexBuildTask.create(row, builder))
        .collect(Collectors.toUnmodifiableList());
  }


  @Override
  public Map<DeferredIndexStatus, Integer> getProgress() {
    return dao.getProgressCounts();
  }
}
