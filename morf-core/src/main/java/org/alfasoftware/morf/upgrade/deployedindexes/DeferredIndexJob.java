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

package org.alfasoftware.morf.upgrade.deployedindexes;

import java.util.Collections;
import java.util.List;

/**
 * One unit of work for the app-side deferred-index executor: a (table,
 * index) pair together with the SQL statements needed to build it.
 *
 * <p>Returned by {@code UpgradePath.getDeferredIndexStatements()}. The
 * app pairs each job's SQL with the matching {@link DeployedIndexTracker}
 * calls — {@code markStarted(tableName, indexName)} /
 * {@code markCompleted(...)} / {@code markFailed(...)} — without having
 * to parse SQL to recover the names.</p>
 *
 * <p>Most dialects return a single CREATE INDEX statement per job
 * ({@code sql.size() == 1}); some dialects (e.g. PostgreSQL with its
 * {@code COMMENT ON INDEX}) return multiple statements per logical index
 * creation — execute them in order.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public final class DeferredIndexJob {

  private final String tableName;
  private final String indexName;
  private final List<String> sql;


  /**
   * @param tableName the table the index belongs to.
   * @param indexName the index name.
   * @param sql the SQL statement(s) to build this one index.
   */
  public DeferredIndexJob(String tableName, String indexName, List<String> sql) {
    this.tableName = tableName;
    this.indexName = indexName;
    this.sql = Collections.unmodifiableList(List.copyOf(sql));
  }


  /** @return the table the index belongs to. */
  public String getTableName() {
    return tableName;
  }


  /** @return the index name. */
  public String getIndexName() {
    return indexName;
  }


  /**
   * @return the SQL statement(s) to build this one index. Execute in
   *     order. Usually one statement; sometimes more (e.g. PostgreSQL
   *     emits {@code COMMENT ON INDEX} alongside the CREATE).
   */
  public List<String> getSql() {
    return sql;
  }
}
