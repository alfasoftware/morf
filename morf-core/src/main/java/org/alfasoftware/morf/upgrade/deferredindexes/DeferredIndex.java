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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;

/**
 * Represents a row in the {@code DeferredIndexes} registration table. Every row
 * is a deferred index — non-deferred indexes are not registered. The lifecycle
 * is {@link DeferredIndexStatus#PENDING} → {@code IN_PROGRESS} →
 * {@code COMPLETED} or {@code FAILED}, with {@code FAILED} non-terminal
 * (re-tried on the next build pass).
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndex {

  /** Primary key. */
  private long id;
  /** Name of the table the registered index belongs to. */
  private String tableName;
  /** Name of the registered index. */
  private String indexName;
  /** Whether the index is declared unique. */
  private boolean indexUnique;
  /** Columns the index covers, in declared order. */
  private List<String> indexColumns;
  /** Lifecycle status: PENDING -&gt; IN_PROGRESS -&gt; COMPLETED or FAILED. */
  private DeferredIndexStatus status;
  /** Number of CREATE attempts for the deferred build. Reset to zero on COMPLETED. */
  private int attemptsCount;
  /** Epoch ms when the registration row was created. */
  private long createdTime;
  /** Epoch ms when the most recent build attempt began, or null before the first attempt. */
  private Long startedTime;
  /** Epoch ms of the last successful COMPLETED transition, or null if never built. */
  private Long completedTime;
  /** Most recent failure message; null when the row has never failed or after the most recent COMPLETED cleared it. */
  private String errorMessage;


  /** @return the primary key. */
  public long getId() {
    return id;
  }

  /** @param id the primary key. */
  public void setId(long id) {
    this.id = id;
  }

  /** @return the name of the table the registered index belongs to. */
  public String getTableName() {
    return tableName;
  }

  /** @param tableName the name of the table the registered index belongs to. */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** @return the name of the registered index. */
  public String getIndexName() {
    return indexName;
  }

  /** @param indexName the name of the registered index. */
  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  /** @return whether the index is declared unique. */
  public boolean isIndexUnique() {
    return indexUnique;
  }

  /** @param indexUnique whether the index is declared unique. */
  public void setIndexUnique(boolean indexUnique) {
    this.indexUnique = indexUnique;
  }

  /** @return the columns the index covers, in declared order. */
  public List<String> getIndexColumns() {
    return indexColumns;
  }

  /** @param indexColumns the columns the index covers, in declared order. */
  public void setIndexColumns(List<String> indexColumns) {
    this.indexColumns = indexColumns;
  }

  /** @return the current lifecycle status. */
  public DeferredIndexStatus getStatus() {
    return status;
  }

  /** @param status the current lifecycle status. */
  public void setStatus(DeferredIndexStatus status) {
    this.status = status;
  }

  /** @return the number of CREATE attempts so far; reset to zero on COMPLETED. */
  public int getAttemptsCount() {
    return attemptsCount;
  }

  /** @param attemptsCount the number of CREATE attempts so far. */
  public void setAttemptsCount(int attemptsCount) {
    this.attemptsCount = attemptsCount;
  }

  /** @return epoch ms when the registration row was created. */
  public long getCreatedTime() {
    return createdTime;
  }

  /** @param createdTime epoch ms when the registration row was created. */
  public void setCreatedTime(long createdTime) {
    this.createdTime = createdTime;
  }

  /** @return epoch ms when the most recent build attempt began, or null before the first attempt. */
  public Long getStartedTime() {
    return startedTime;
  }

  /** @param startedTime epoch ms when the most recent build attempt began. */
  public void setStartedTime(Long startedTime) {
    this.startedTime = startedTime;
  }

  /** @return epoch ms of the last successful COMPLETED transition, or null if never built. */
  public Long getCompletedTime() {
    return completedTime;
  }

  /** @param completedTime epoch ms of the last successful COMPLETED transition. */
  public void setCompletedTime(Long completedTime) {
    this.completedTime = completedTime;
  }

  /** @return the most recent failure message, or null when the row has never failed or after the most recent COMPLETED cleared it. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** @param errorMessage the most recent failure message; null clears any prior message. */
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
    return builder.deferred();
  }
}
