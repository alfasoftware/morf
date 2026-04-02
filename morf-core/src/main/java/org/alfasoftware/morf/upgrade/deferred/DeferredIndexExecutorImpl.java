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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexExecutor}.
 *
 * <p>Scans the database schema for indexes with {@code isDeferred()=true}
 * (virtual indexes declared in table comments but not yet physically built)
 * and creates them using
 * {@link org.alfasoftware.morf.jdbc.SqlDialect#deferredIndexDeploymentStatements(Table, Index)}.
 * </p>
 *
 * <p>Retry logic uses a fixed delay between attempts, up to
 * {@link UpgradeConfigAndContext#getDeferredIndexMaxRetries()} additional
 * attempts after the first failure.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexExecutorImpl implements DeferredIndexExecutor {

  private static final Log log = LogFactory.getLog(DeferredIndexExecutorImpl.class);

  /** Fixed delay in milliseconds between retry attempts. */
  private static final long RETRY_DELAY_MS = 5_000L;

  private final ConnectionResources connectionResources;
  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final UpgradeConfigAndContext config;
  private final DeferredIndexExecutorServiceFactory executorServiceFactory;

  /** The worker thread pool; may be null if execution has not started. */
  private volatile ExecutorService threadPool;


  /**
   * Constructs an executor using the supplied dependencies.
   *
   * @param connectionResources      database connection resources.
   * @param sqlScriptExecutorProvider provider for SQL script executors.
   * @param config                   upgrade configuration.
   * @param executorServiceFactory    factory for creating the worker thread pool.
   */
  @Inject
  DeferredIndexExecutorImpl(ConnectionResources connectionResources,
                            SqlScriptExecutorProvider sqlScriptExecutorProvider,
                            UpgradeConfigAndContext config,
                            DeferredIndexExecutorServiceFactory executorServiceFactory) {
    this.connectionResources = connectionResources;
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.config = config;
    this.executorServiceFactory = executorServiceFactory;
  }


  /**
   * @see DeferredIndexExecutor#execute()
   */
  @Override
  public CompletableFuture<Void> execute() {
    if (!config.isDeferredIndexCreationEnabled()) {
      log.debug("Deferred index creation is disabled -- skipping execution");
      return CompletableFuture.completedFuture(null);
    }

    if (threadPool != null) {
      log.fatal("execute() called more than once on DeferredIndexExecutorImpl");
      throw new IllegalStateException("DeferredIndexExecutor.execute() has already been called");
    }

    validateExecutorConfig();

    List<DeferredIndexEntry> missing = findMissingDeferredIndexes();

    if (missing.isEmpty()) {
      log.info("No deferred indexes to build.");
      return CompletableFuture.completedFuture(null);
    }

    log.info("Found " + missing.size() + " deferred index(es) to build.");

    threadPool = executorServiceFactory.create(config.getDeferredIndexThreadPoolSize());

    AtomicInteger completed = new AtomicInteger();
    int total = missing.size();

    CompletableFuture<?>[] futures = missing.stream()
        .map(entry -> CompletableFuture.runAsync(() -> {
          executeWithRetry(entry);
          log.info("Deferred index progress: " + completed.incrementAndGet() + "/" + total + " complete.");
        }, threadPool))
        .toArray(CompletableFuture[]::new);

    return CompletableFuture.allOf(futures)
        .whenComplete((v, t) -> {
          threadPool.shutdown();
          threadPool = null;
          log.info("Deferred index execution complete.");
        });
  }


  /**
   * @see DeferredIndexExecutor#getMissingDeferredIndexStatements()
   */
  @Override
  public List<String> getMissingDeferredIndexStatements() {
    if (!config.isDeferredIndexCreationEnabled()) {
      return Collections.emptyList();
    }

    List<DeferredIndexEntry> missing = findMissingDeferredIndexes();
    List<String> statements = new ArrayList<>();
    for (DeferredIndexEntry entry : missing) {
      statements.addAll(
          connectionResources.sqlDialect().deferredIndexDeploymentStatements(entry.table, entry.index));
    }
    return statements;
  }


  // -------------------------------------------------------------------------
  // Internal: schema scanning
  // -------------------------------------------------------------------------

  /**
   * Scans the database schema for deferred indexes that have not yet been
   * physically built. An index with {@code isDeferred()=true} in the schema
   * returned by the MetaDataProvider indicates it was declared in a table
   * comment but does not yet exist as a physical index.
   *
   * @return list of table/index pairs to build.
   */
  private List<DeferredIndexEntry> findMissingDeferredIndexes() {
    List<DeferredIndexEntry> result = new ArrayList<>();

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      for (Table table : sr.tables()) {
        for (Index index : table.indexes()) {
          if (index.isDeferred()) {
            log.debug("Found unbuilt deferred index [" + index.getName()
                + "] on table [" + table.getName() + "]");
            result.add(new DeferredIndexEntry(table, index));
          }
        }
      }
    }

    return result;
  }


  // -------------------------------------------------------------------------
  // Internal: execution logic
  // -------------------------------------------------------------------------

  /**
   * Attempts to build the index for a single entry, retrying with a fixed
   * delay on failure up to {@link UpgradeConfigAndContext#getDeferredIndexMaxRetries()}
   * times.
   *
   * @param entry the table/index pair to build.
   */
  private void executeWithRetry(DeferredIndexEntry entry) {
    int maxAttempts = config.getDeferredIndexMaxRetries() + 1;

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      log.info("Building deferred index [" + entry.index.getName() + "] on table ["
          + entry.table.getName() + "], attempt " + (attempt + 1) + "/" + maxAttempts);
      long startTime = System.currentTimeMillis();

      try {
        buildIndex(entry);
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        log.info("Deferred index [" + entry.index.getName() + "] on table ["
            + entry.table.getName() + "] completed in " + elapsedSeconds + " s");
        return;

      } catch (Exception e) {
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        int nextAttempt = attempt + 1;

        if (nextAttempt < maxAttempts) {
          log.error("Deferred index [" + entry.index.getName() + "] on table ["
              + entry.table.getName() + "] failed after " + elapsedSeconds
              + " s (attempt " + nextAttempt + "/" + maxAttempts + "), will retry: "
              + e.getMessage());
          sleepForRetry();
        } else {
          log.error("Deferred index [" + entry.index.getName() + "] on table ["
              + entry.table.getName() + "] permanently failed after " + elapsedSeconds
              + " s (" + nextAttempt + " attempt(s))", e);
        }
      }
    }
  }


  /**
   * Executes the {@code CREATE INDEX} DDL for the given entry using an
   * autocommit connection. Autocommit is required for PostgreSQL's
   * {@code CREATE INDEX CONCURRENTLY}.
   *
   * @param entry the table/index pair containing the metadata.
   */
  private void buildIndex(DeferredIndexEntry entry) {
    Collection<String> statements = connectionResources.sqlDialect()
        .deferredIndexDeploymentStatements(entry.table, entry.index);

    try (Connection connection = connectionResources.getDataSource().getConnection()) {
      boolean wasAutoCommit = connection.getAutoCommit();
      try {
        connection.setAutoCommit(true);
        sqlScriptExecutorProvider.get().execute(statements, connection);
      } finally {
        connection.setAutoCommit(wasAutoCommit);
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error building deferred index " + entry.index.getName(), e);
    }
  }


  /**
   * Sleeps for a fixed delay between retry attempts.
   */
  private void sleepForRetry() {
    try {
      Thread.sleep(RETRY_DELAY_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }


  /**
   * Validates executor-relevant configuration values.
   */
  private void validateExecutorConfig() {
    if (config.getDeferredIndexThreadPoolSize() < 1) {
      throw new IllegalArgumentException(
          "deferredIndexThreadPoolSize must be >= 1, was " + config.getDeferredIndexThreadPoolSize());
    }
    if (config.getDeferredIndexMaxRetries() < 0) {
      throw new IllegalArgumentException(
          "deferredIndexMaxRetries must be >= 0, was " + config.getDeferredIndexMaxRetries());
    }
  }


  // -------------------------------------------------------------------------
  // Inner class
  // -------------------------------------------------------------------------

  /**
   * Pairs a {@link Table} with an {@link Index} that needs to be built.
   */
  private static final class DeferredIndexEntry {

    /** The table the index belongs to. */
    final Table table;

    /** The deferred index to build. */
    final Index index;

    DeferredIndexEntry(Table table, Index index) {
      this.table = table;
      this.index = index;
    }
  }
}
