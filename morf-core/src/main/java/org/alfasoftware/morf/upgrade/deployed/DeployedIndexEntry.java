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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;

/**
 * Represents a row in the DeployedIndexes table, tracking both deferred
 * and non-deferred indexes.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeployedIndexEntry {

  private long id;
  private String upgradeUUID;
  private String tableName;
  private String indexName;
  private boolean indexUnique;
  private List<String> indexColumns;
  private boolean indexDeferred;
  private DeployedIndexStatus status;
  private int retryCount;
  private long createdTime;
  private Long startedTime;
  private Long completedTime;
  private String errorMessage;


  /** @see #id */
  public long getId() {
    return id;
  }

  /** @see #id */
  public void setId(long id) {
    this.id = id;
  }

  /** @see #upgradeUUID */
  public String getUpgradeUUID() {
    return upgradeUUID;
  }

  /** @see #upgradeUUID */
  public void setUpgradeUUID(String upgradeUUID) {
    this.upgradeUUID = upgradeUUID;
  }

  /** @see #tableName */
  public String getTableName() {
    return tableName;
  }

  /** @see #tableName */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** @see #indexName */
  public String getIndexName() {
    return indexName;
  }

  /** @see #indexName */
  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  /** @see #indexUnique */
  public boolean isIndexUnique() {
    return indexUnique;
  }

  /** @see #indexUnique */
  public void setIndexUnique(boolean indexUnique) {
    this.indexUnique = indexUnique;
  }

  /** @see #indexColumns */
  public List<String> getIndexColumns() {
    return indexColumns;
  }

  /** @see #indexColumns */
  public void setIndexColumns(List<String> indexColumns) {
    this.indexColumns = indexColumns;
  }

  /** @see #indexDeferred */
  public boolean isIndexDeferred() {
    return indexDeferred;
  }

  /** @see #indexDeferred */
  public void setIndexDeferred(boolean indexDeferred) {
    this.indexDeferred = indexDeferred;
  }

  /** @see #status */
  public DeployedIndexStatus getStatus() {
    return status;
  }

  /** @see #status */
  public void setStatus(DeployedIndexStatus status) {
    this.status = status;
  }

  /** @see #retryCount */
  public int getRetryCount() {
    return retryCount;
  }

  /** @see #retryCount */
  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  /** @see #createdTime */
  public long getCreatedTime() {
    return createdTime;
  }

  /** @see #createdTime */
  public void setCreatedTime(long createdTime) {
    this.createdTime = createdTime;
  }

  /** @see #startedTime */
  public Long getStartedTime() {
    return startedTime;
  }

  /** @see #startedTime */
  public void setStartedTime(Long startedTime) {
    this.startedTime = startedTime;
  }

  /** @see #completedTime */
  public Long getCompletedTime() {
    return completedTime;
  }

  /** @see #completedTime */
  public void setCompletedTime(Long completedTime) {
    this.completedTime = completedTime;
  }

  /** @see #errorMessage */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** @see #errorMessage */
  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }


  /**
   * Reconstructs an {@link Index} metadata object from this entry.
   *
   * @return an Index with the name, columns, and uniqueness from this entry.
   */
  public Index toIndex() {
    IndexBuilder builder = index(indexName).columns(indexColumns.toArray(new String[0]));
    if (indexUnique) {
      builder = builder.unique();
    }
    if (indexDeferred) {
      builder = builder.deferred();
    }
    return builder;
  }


  /**
   * Parses a comma-separated column string into a list.
   *
   * @param columnsCsv the comma-separated column names.
   * @return the parsed list.
   */
  public static List<String> parseColumns(String columnsCsv) {
    return Arrays.asList(columnsCsv.split(","));
  }


  /**
   * Joins a list of column names into a comma-separated string.
   *
   * @param columns the column names.
   * @return the joined string.
   */
  public static String joinColumns(List<String> columns) {
    return String.join(",", columns);
  }
}
