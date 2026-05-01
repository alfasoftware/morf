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

import org.alfasoftware.morf.jdbc.ConnectionResources;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Default implementation of {@link DeferredIndexService}. Reads non-{@code
 * COMPLETED} rows from {@link DeferredIndexesDAO} and wraps each in a
 * {@link DeferredIndexBuildTaskImpl}; progress reads delegate straight to the
 * DAO.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexServiceImpl implements DeferredIndexService {

  private final ConnectionResources connectionResources;
  private final DeferredIndexesDAO dao;


  @Inject
  DeferredIndexServiceImpl(ConnectionResources connectionResources, DeferredIndexesDAO dao) {
    this.connectionResources = connectionResources;
    this.dao = dao;
  }


  @Override
  public List<DeferredIndexBuildTask> getBuildTasks() {
    return dao.findNonTerminal().stream()
        .map(row -> DeferredIndexBuildTask.create(row, connectionResources, dao))
        .collect(Collectors.toUnmodifiableList());
  }


  @Override
  public Map<DeferredIndexStatus, Integer> getProgress() {
    return dao.getProgressCounts();
  }
}
