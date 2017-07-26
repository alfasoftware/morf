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

package org.alfasoftware.morf.testing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.DatabaseDataSetProducer;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.upgrade.ViewChanges;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

/**
 * Abstracts the task of managing the database schema from the testing code.
 *
 * <p>It maintains an internal cache of the state of the database, so repeated requests to change state are efficient - if there are
 * no changes to make it can determine this extremely efficiently.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class DatabaseSchemaManager {
  private static final Log log = LogFactory.getLog(DatabaseSchemaManager.class);

  /**
   * Controls how truncation happens
   */
  public enum TruncationBehavior {
    /**
     * Always truncate every table, even if it has not been modified.
     */
    ALWAYS,
    /**
     * Only truncates if the table's schema has changed.
     */
    ONLY_ON_TABLE_CHANGE
  }

  // All our cached information about the database is static, because the underlying database is static.
  // If we are ever initialised with a different set of connection details, meaning we're connecting to
  // a different underlying database, we invalidate the cache and start again.
  private static ConnectionResources connectionResources;
  private static SqlDialect dialect;
  private static final Map<String, Table> tables = Maps.newHashMap();
  private static final Map<String, View> views = Maps.newHashMap();
  private static final Set<String> tablesNotNeedingTruncate = Sets.newHashSet();
  private static boolean tablesLoaded;
  private static boolean viewsLoaded;

  private final DataSource dataSource;
  private final SqlScriptExecutorProvider executor;


  /**
   * Injected constructor.
   * @param connectionResources The connection to use
   * @param dataSource The data source
   * @param executor The script executor
   */
  @Inject
  protected DatabaseSchemaManager(ConnectionResources connectionResources, DataSource dataSource, SqlScriptExecutorProvider executor) {
    super();

    // Allow for the possibility that we might be connecting to a different database, so will
    // need to invalidate and refetch the schema.
    if (!connectionResources.equals(DatabaseSchemaManager.connectionResources)) {
      if (log.isDebugEnabled()) {
        log.debug("New connection details. Refreshing schema: " + connectionResources);
      }
      DatabaseSchemaManager.connectionResources = connectionResources;
      DatabaseSchemaManager.dialect = connectionResources.sqlDialect();
      invalidateCache();
    }

    this.dataSource = dataSource;
    this.executor = executor;
  }


  /**
   * Mutates the current database schema so that it supports the one requested.
   *
   * <p>When this method returns, it guarantees that all the tables in {code schema} are
   * present in the database and also empty.</p>
   *
   * <p>Note it does not guarantee that no other tables exist.</p>
   *
   * @param schema The schema which the database should support
   * @param truncationBehavior The behaviour to use when an existing table is found. Should it be truncated?
   */
  public void mutateToSupportSchema(Schema schema, TruncationBehavior truncationBehavior) {
    ProducerCache producerCache = new ProducerCache();
    try {

      // Drop all views in the schema and create the ones we need.
      ViewChanges changes = new ViewChanges(
        schema.views(),
        viewCache(producerCache).values(),
        schema.views()
      );

      Collection<String> sql = Lists.newLinkedList();

      for (View view : changes.getViewsToDrop()) {
        sql.addAll(dropViewIfExists(view));
      }
      sql.addAll(ensureTablesExist(schema, truncationBehavior, producerCache));

      for (View view: changes.getViewsToDeploy()) {
        sql.addAll(deployView(view));
      }

      executeScript(sql);

    } catch (RuntimeException e) {
      if (log.isDebugEnabled()) {
        log.debug("Invalidating cache. Exception while mutating schema.");
      }
      invalidateCache();
      throw e;
    } finally {
      producerCache.close();
    }
  }


  /**
   * Returns the cached set of tables in the database.
   */
  private Map<String, Table> tableCache(ProducerCache producerCache) {
    if (!tablesLoaded) {
      cacheTables(producerCache.get().getSchema().tables());
    }
    return tables;
  }


  private void cacheTables(Iterable<Table> newTables) {
    // Create disconnected copies of the tables in case we run across connections/data sources
    Iterable<Table> copies = Iterables.transform(newTables, new CopyTables());
    tables.putAll(Maps.uniqueIndex(copies, new Function<Table, String>() {
      @Override
      public String apply(Table table) {
        return table.getName().toUpperCase();
      }
    }));
    tablesLoaded = true;
  }


  /**
   * Returns the cached set of views in the database.
   */
  private Map<String, View> viewCache(ProducerCache producerCache) {
    if (!viewsLoaded) {
      cacheViews(producerCache.get().getSchema().views());
    }
    return views;
  }


  private void cacheViews(Iterable<View> newViews) {
    // Create disconnected copies of the views in case we run across connections/data sources
    Iterable<View> copies = Iterables.transform(newViews, new CopyViews());
    views.putAll(Maps.uniqueIndex(copies, new Function<View, String>() {
      @Override
      public String apply(View view) {
        return view.getName().toUpperCase();
      }
    }));
    viewsLoaded = true;
  }


  private Table getTable(ProducerCache producerCache, String name) {
    return tableCache(producerCache).get(name.toUpperCase());
  }


  /**
   * Invalidate the cache of database tables. Use when the schema has changed underneath this schema manager.
   */
  public void invalidateCache() {
    if (log.isDebugEnabled()) {
      StackTraceElement stack = new Throwable().getStackTrace()[1];
      log.debug("Cache invalidated at " + stack.getClassName() + "." + stack.getMethodName() + ":" + stack.getLineNumber());
    }
    clearCache();
  }


  private void clearCache() {
    tables.clear();
    views.clear();
    tablesNotNeedingTruncate.clear();
    tablesLoaded = false;
    viewsLoaded = false;
  }


  /**
   * Drop the specified tables from the schema if they are present.
   *
   * @param tablesToDrop The tables to delete if they are present in the database.
   */
  public void dropTablesIfPresent(Set<String> tablesToDrop) {
    ProducerCache producerCache = new ProducerCache();
    try {
      Collection<String> sql = Lists.newLinkedList();
      for (String tableName : tablesToDrop) {
        Table cachedTable = getTable(producerCache, tableName);
        if (cachedTable != null) {
          sql.addAll(dropTable(cachedTable));
        }
      }
      executeScript(sql);
    } finally {
      producerCache.close();
    }
  }


  /**
   * Drop all tables so that the schema is empty.
   */
  public void dropAllTables() {
    ProducerCache producerCache = new ProducerCache();
    try {
      Schema databaseSchema = producerCache.get().getSchema();
      ImmutableList<Table> tablesToDrop = ImmutableList.copyOf(databaseSchema.tables());
      List<String> script = Lists.newArrayList();
      for (Table table : tablesToDrop) {
        for (String sql : dialect.dropStatements(table)) {
          script.add(sql);
        }
      }
      executeScript(script);
    } finally {
      producerCache.close();
    }
    tables.clear();
    tablesNotNeedingTruncate.clear();
  }


  private void executeScript(Collection<String> script) {
    if (!script.isEmpty()) {
      executor.get().execute(script);
    }
  }


  /**
   * Drop all views.
   */
  public void dropAllViews() {
    ProducerCache producerCache = new ProducerCache();
    try {
      Schema databaseSchema = producerCache.get().getSchema();
      ImmutableList<View> viewsToDrop = ImmutableList.copyOf(databaseSchema.views());
      List<String> script = Lists.newArrayList();
      for (View view : viewsToDrop) {
        for (String sql : dialect.dropStatements(view)) {
          script.add(sql);
        }
      }
      executeScript(script);
    } finally {
      producerCache.close();
    }
    views.clear();
  }


  /**
   * Ensure that every table in the schema is present in the DB.
   */
  private Collection<String> ensureTablesExist(Schema schema, TruncationBehavior truncationBehavior, ProducerCache producerCache) {
    Collection<String> sql = Lists.newLinkedList();
    for (Table requiredTable : schema.tables()) {
      sql.addAll(ensureTableExists(requiredTable, truncationBehavior, producerCache));
    }
    return sql;
  }


  /**
   * Ensure that a specific table is present in the DB.
   *
   * @return Any SQL required to adjust the DB to include this table.
   */
  private Collection<? extends String> ensureTableExists(Table requiredTable, TruncationBehavior truncationBehavior, ProducerCache producerCache) {

    boolean dropRequired;
    boolean deployRequired;
    boolean truncateRequired;

    DifferenceWriter differenceWriter = new DifferenceWriter() {
      @Override
      public void difference(String message) {
        log.debug(message);
      }
    };

    // if we have an existing table, check it's identical
    Table existingTable = getTable(producerCache, requiredTable.getName());
    if (existingTable != null) {
      if (new SchemaHomology(differenceWriter, "cache", "required").tablesMatch(existingTable, requiredTable)) {
        // they match - it's identical, so we can re-use it
        dropRequired = false;
        deployRequired = false;
        if (tablesNotNeedingTruncate.contains(requiredTable.getName().toUpperCase())) {
          truncateRequired =  TruncationBehavior.ALWAYS.equals(truncationBehavior);
        } else {
          // if we didn't find it in the cache we don't know what state it is in, so truncate it
          truncateRequired = true;
          tablesNotNeedingTruncate.add(requiredTable.getName().toUpperCase());
        }
      } else {
        // they don't match
        dropRequired = true;
        deployRequired = true;
        truncateRequired = false;
      }
    } else {
      // no existing table
      dropRequired = false;
      deployRequired = true;
      truncateRequired = false;
    }

    Collection<String> sql = Lists.newLinkedList();

    if (dropRequired)
      sql.addAll(dropTable(existingTable));

    if (deployRequired) {
      sql.addAll(deployTable(requiredTable));
    }

    if (truncateRequired) {
      sql.addAll(truncateTable(requiredTable));
    }

    return sql;
  }


  /**
   * Deploys the specified table to the database.
   *
   * @param table the table to deploy
   * @param connectionResources the database to drop it from
   */
  private Collection<String> deployTable(Table table) {
    if (log.isDebugEnabled()) log.debug("Deploying table [" + table.getName() + "]");
    String upperCase = table.getName().toUpperCase();
    tables.put(upperCase, SchemaUtils.copy(table));
    tablesNotNeedingTruncate.add(upperCase);
    return dialect.tableDeploymentStatements(table);
  }


  /**
   * Removes the specified view from the database, if it exists.   Otherwise
   * do nothing.  To allow for JDBC implem,entations that do not support
   * conditional dropping of views, this will trap and ignore
   *
   * @param view the view to drop
   */
  private Collection<String> dropViewIfExists(View view) {
    if (log.isDebugEnabled()) log.debug("Dropping any existing view [" + view.getName() + "]");
    views.remove(view.getName().toUpperCase());
    return dialect.dropStatements(view);
  }


  /**
   * Deploys the specified view to the database.
   *
   * @param view the view to deploy
   * @param connectionResources the database to drop it from
   */
  private Collection<String> deployView(View view) {
    if (log.isDebugEnabled()) log.debug("Deploying view [" + view.getName() + "]");
    views.put(view.getName().toUpperCase(), SchemaUtils.copy(view));
    return dialect.viewDeploymentStatements(view);
  }


  /**
   * Drops a table and all its dependencies (e.g. indexes).
   *
   * @param table the table to drop
   * @param connectionResources the database to drop it from
   * @return
   */
  private Collection<String> dropTable(Table table) {
    if (log.isDebugEnabled()) log.debug("Dropping table [" + table.getName() + "]");
    String upperCase = table.getName().toUpperCase();
    tables.remove(upperCase);
    tablesNotNeedingTruncate.remove(upperCase);
    return dialect.dropStatements(table);
  }


  /**
   * Truncates the specified table.
   *
   * @param table the table to truncate.
   * @param connectionResources the database on which the table exists.
   */
  private Collection<String> truncateTable(Table table) {
    if (log.isDebugEnabled()) log.debug("Truncating table [" + table.getName() + "]");

    // use delete-all rather than truncate, because at least on Oracle this is a lot faster when the table is small.
    return dialect.deleteAllFromTableStatements(table);
  }


  /**
   * Caches the {@link DatabaseDataSetProducer}, and only creates it lazily. This is required because a {@link DatabaseDataSetProducer} can be expensive to create.
   */
  private class ProducerCache {
    private DatabaseDataSetProducer producer;

    DatabaseDataSetProducer get() {
      if (producer == null) {
        producer = new DatabaseDataSetProducer(connectionResources, dataSource);
        producer.open();
      }
      return producer;
    }


    void close() {
      if (producer != null) {
        producer.close();
      }
    }
  }


  /**
   * Function which creates copies of views.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  private static class CopyViews implements Function<View, View> {
    @Override
    public View apply(View view) {
      return SchemaUtils.copy(view);
    }
  }


  /**
   * Function which creates copies of tables.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  private static class CopyTables implements Function<Table, Table> {
    @Override
    public Table apply(Table table) {
      return SchemaUtils.copy(table);
    }
  }
}
