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

package org.alfasoftware.morf.upgrade.deployed;

import java.util.List;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Default implementation of {@link DeployedIndexTracker} backed by the
 * {@link DeployedIndexesDAO}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeployedIndexTrackerImpl implements DeployedIndexTracker {

  private final DeployedIndexesDAO dao;


  /**
   * Constructs the tracker.
   *
   * @param dao DAO for DeployedIndexes operations.
   */
  @Inject
  DeployedIndexTrackerImpl(DeployedIndexesDAO dao) {
    this.dao = dao;
  }


  @Override
  public void markStarted(String tableName, String indexName) {
    dao.markStarted(tableName, indexName, System.currentTimeMillis());
  }


  @Override
  public void markCompleted(String tableName, String indexName) {
    dao.markCompleted(tableName, indexName, System.currentTimeMillis());
  }


  @Override
  public void markFailed(String tableName, String indexName, String errorMessage) {
    dao.markFailed(tableName, indexName, errorMessage);
  }


  @Override
  public Map<DeployedIndexStatus, Integer> getProgress() {
    return dao.countAllByStatus();
  }


  @Override
  public List<DeployedIndexEntry> getPendingIndexes() {
    return dao.findNonTerminalOperations();
  }
}
