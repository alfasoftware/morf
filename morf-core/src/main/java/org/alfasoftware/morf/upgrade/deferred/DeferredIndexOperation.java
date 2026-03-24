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

package org.alfasoftware.morf.upgrade.deferred;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;

/**
 * Represents a row in the {@code DeferredIndexOperation} table.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
class DeferredIndexOperation {


  /**
   * Unique identifier for this operation.
   */
  private long id;

  /**
   * UUID of the {@code UpgradeStep} that created this operation.
   */
  private String upgradeUUID;

  /**
   * Name of the table on which the index operation is to be applied.
   */
  private String tableName;

  /**
   * Name of the index to be created or modified.
   */
  private String indexName;

  /**
   * Whether the index should be unique.
   */
  private boolean indexUnique;

  /**
   * Current status of this operation.
   */
  private DeferredIndexStatus status;

  /**
   * Number of retry attempts made so far.
   */
  private int retryCount;

  /**
   * Time at which this operation was created, stored as epoch milliseconds.
   */
  private long createdTime;

  /**
   * Time at which execution started, stored as epoch milliseconds. Null if not yet started.
   */
  private Long startedTime;

  /**
   * Time at which execution completed, stored as epoch milliseconds. Null if not yet completed.
   */
  private Long completedTime;

  /**
   * Error message if the operation has failed. Null if not failed.
   */
  private String errorMessage;

  /**
   * Ordered list of column names making up the index.
   */
  private List<String> columnNames;


  /**
   * @see #id
   */
  public long getId() {
    return id;
  }


  /**
   * @see #id
   */
  public void setId(long id) {
    this.id = id;
  }


  /**
   * @see #upgradeUUID
   */
  public String getUpgradeUUID() {
    return upgradeUUID;
  }


  /**
   * @see #upgradeUUID
   */
  public void setUpgradeUUID(String upgradeUUID) {
    this.upgradeUUID = upgradeUUID;
  }


  /**
   * @see #tableName
   */
  public String getTableName() {
    return tableName;
  }


  /**
   * @see #tableName
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }


  /**
   * @see #indexName
   */
  public String getIndexName() {
    return indexName;
  }


  /**
   * @see #indexName
   */
  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }


  /**
   * @see #indexUnique
   */
  public boolean isIndexUnique() {
    return indexUnique;
  }


  /**
   * @see #indexUnique
   */
  public void setIndexUnique(boolean indexUnique) {
    this.indexUnique = indexUnique;
  }


  /**
   * @see #status
   */
  public DeferredIndexStatus getStatus() {
    return status;
  }


  /**
   * @see #status
   */
  public void setStatus(DeferredIndexStatus status) {
    this.status = status;
  }


  /**
   * @see #retryCount
   */
  public int getRetryCount() {
    return retryCount;
  }


  /**
   * @see #retryCount
   */
  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }


  /**
   * @see #createdTime
   */
  public long getCreatedTime() {
    return createdTime;
  }


  /**
   * @see #createdTime
   */
  public void setCreatedTime(long createdTime) {
    this.createdTime = createdTime;
  }


  /**
   * @see #startedTime
   */
  public Long getStartedTime() {
    return startedTime;
  }


  /**
   * @see #startedTime
   */
  public void setStartedTime(Long startedTime) {
    this.startedTime = startedTime;
  }


  /**
   * @see #completedTime
   */
  public Long getCompletedTime() {
    return completedTime;
  }


  /**
   * @see #completedTime
   */
  public void setCompletedTime(Long completedTime) {
    this.completedTime = completedTime;
  }


  /**
   * @see #errorMessage
   */
  public String getErrorMessage() {
    return errorMessage;
  }


  /**
   * @see #errorMessage
   */
  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }


  /**
   * @see #columnNames
   */
  public List<String> getColumnNames() {
    return columnNames;
  }


  /**
   * @see #columnNames
   */
  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }


  /**
   * Reconstructs an {@link Index} metadata object from this operation's
   * index name, uniqueness flag, and column names.
   *
   * @return the reconstructed index.
   */
  Index toIndex() {
    IndexBuilder builder = index(indexName);
    if (indexUnique) {
      builder = builder.unique();
    }
    return builder.columns(columnNames.toArray(new String[0]));
  }
}
