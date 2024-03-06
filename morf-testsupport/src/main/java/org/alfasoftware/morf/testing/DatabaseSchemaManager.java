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

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.upgrade.SequenceChanges;
import org.alfasoftware.morf.upgrade.ViewChanges;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
  // All details are stored as ThreadLocals to enable multithreaded testing where each Thread has its own database connection.
  private static final ThreadLocal<ConnectionResources> connectionResources = new ThreadLocal<>();
  private static final ThreadLocal<SqlDialect> dialect = new ThreadLocal<>();
  private static final ThreadLocal<Map<String, Table>> tables = ThreadLocal.withInitial(HashMap::new);
  private static final ThreadLocal<Map<String, View>> views = ThreadLocal.withInitial(HashMap::new);
  private static final ThreadLocal<Map<String, Sequence>> sequences = ThreadLocal.withInitial(HashMap::new);
  private static final ThreadLocal<Set<String>> tablesNotNeedingTruncate = ThreadLocal.withInitial(HashSet::new);
  private static final ThreadLocal<Set<String>> viewsDeployedByThis = ThreadLocal.withInitial(HashSet::new);
  private static final ThreadLocal<Set<String>> sequencesDeployedByThis = ThreadLocal.withInitial(HashSet::new);
  private static final ThreadLocal<Boolean> tablesLoaded = ThreadLocal.withInitial(() -> false);
  private static final ThreadLocal<Boolean> viewsLoaded = ThreadLocal.withInitial(() -> false);
  private static final ThreadLocal<Boolean> sequencesLoaded = ThreadLocal.withInitial(() -> false);

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
    if (!connectionResources.equals(DatabaseSchemaManager.connectionResources.get())) {
      if (log.isDebugEnabled()) {
        log.debug("New connection details. Refreshing schema: " + connectionResources);
      }
      DatabaseSchemaManager.connectionResources.set(connectionResources);
      DatabaseSchemaManager.dialect.set(connectionResources.sqlDialect());
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

      Collection<String> tableStatements = ensureTablesExist(schema, truncationBehavior, producerCache);
      if (!tableStatements.isEmpty()) {
        viewsDeployedByThis.get().clear(); // this will force a drop and redeploy, needed in case the views are affected.
      }

      // Drop all views in the schema and create the ones we need.
      // note that if this class deployed the view already, then leave it alone as it means the view must be based on the current definition
      Collection<View> viewsToDrop = viewCache(producerCache).values().stream().filter(v->!viewsDeployedByThis.get().contains(v.getName().toUpperCase())).collect(toList());
      Collection<View> viewToDeploy = schema.views().stream().filter(v->!viewsDeployedByThis.get().contains(v.getName().toUpperCase())).collect(toList());;
      ViewChanges changes = new ViewChanges(
        schema.views(),
        viewsToDrop,
        viewToDeploy
      );

      // Drop all sequences in the schema and create the ones we need.
      // note that if this class deployed the sequence already, then leave it alone as it means the sequence must be based on the current definition
      Collection<Sequence> sequencesToDrop = sequenceCache(producerCache).values().stream().filter(s->!sequencesDeployedByThis.get().contains(s.getName().toUpperCase())).collect(toList());
      Collection<Sequence> sequencesToDeploy = schema.sequences().stream().filter(s->!sequencesDeployedByThis.get().contains(s.getName().toUpperCase())).collect(toList());
      SequenceChanges sequenceChanges = new SequenceChanges(
        schema.sequences(),
        sequencesToDrop,
        sequencesToDeploy
      );

      Collection<String> sql = Lists.newLinkedList();

      for (View view : changes.getViewsToDeploy()) {
        sql.addAll(dropTableIfPresent(producerCache, view.getName()));
      }

      for (View view : changes.getViewsToDrop()) {
        sql.addAll(dropViewIfExists(view));
      }

      sql.addAll(tableStatements);

      for (View view: changes.getViewsToDeploy()) {
        sql.addAll(deployView(view));
      }

      for (Sequence sequence : sequenceChanges.getSequencesToDrop()) {
        sql.addAll(dropSequenceIfExists(sequence));
      }

      for (Sequence sequence : sequenceChanges.getSequencesToDeploy()) {
        sql.addAll(deploySequence(sequence));
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
    if (!tablesLoaded.get()) {
      cacheTables(producerCache.get().getSchema().tables());
    }
    return tables.get();
  }


  private void cacheTables(Iterable<Table> newTables) {
    // Create disconnected copies of the tables in case we run across connections/data sources
    Iterable<Table> copies = Iterables.transform(newTables, new CopyTables());
    tables.get().putAll(Maps.uniqueIndex(copies, new Function<Table, String>() {
      @Override
      public String apply(Table table) {
        return table.getName().toUpperCase();
      }
    }));
    tablesLoaded.set(true);
  }


  /**
   * Returns the cached set of views in the database.
   */
  private Map<String, View> viewCache(ProducerCache producerCache) {
    if (!viewsLoaded.get()) {
      cacheViews(producerCache.get().getSchema().views());
    }
    return views.get();
  }


  private void cacheViews(Iterable<View> newViews) {
    // Create disconnected copies of the views in case we run across connections/data sources
    Iterable<View> copies = Iterables.transform(newViews, new CopyViews());
    views.get().putAll(Maps.uniqueIndex(copies, new Function<View, String>() {
      @Override
      public String apply(View view) {
        return view.getName().toUpperCase();
      }
    }));
    viewsLoaded.set(true);
  }


  /**
   * Returns the cached set of sequences in the database.
   */
  private Map<String, Sequence> sequenceCache(ProducerCache producerCache) {
    if (!sequencesLoaded.get()) {
      cacheSequences(producerCache.get().getSchema().sequences());
    }
    return sequences.get();
  }


  private void cacheSequences(Iterable<Sequence> newSequences) {
    // Create disconnected copies of the sequences in case we run across connections/data sources
    Iterable<Sequence> copies = Iterables.transform(newSequences, new CopySequences());
    sequences.get().putAll(Maps.uniqueIndex(copies, new Function<Sequence, String>() {
      @Override
      public String apply(Sequence sequence) {
        return sequence.getName().toUpperCase();
      }
    }));
    sequencesLoaded.set(true);
  }


  private Table getTable(ProducerCache producerCache, String name) {
    return tableCache(producerCache).get(name.toUpperCase());
  }


  /**
   * Invalidate the cache of database tables. Use when the schema has changed underneath this schema manager.
   */
  public final void invalidateCache() {
    if (log.isDebugEnabled()) {
      StackTraceElement stack = new Throwable().getStackTrace()[1];
      log.debug("Cache invalidated at " + stack.getClassName() + "." + stack.getMethodName() + ":" + stack.getLineNumber());
    }
    clearCache();
  }


  private void clearCache() {
    tables.get().clear();
    views.get().clear();
    sequences.get().clear();
    tablesNotNeedingTruncate.get().clear();
    tablesLoaded.set(false);
    viewsLoaded.set(false);
    sequencesLoaded.set(false);
    viewsDeployedByThis.get().clear();
    sequencesDeployedByThis.get().clear();
  }


  /**
   * Drop the specified tables from the schema if they are present.
   *
   * @param producerCache database dataset producer cache
   * @param tableName table name to drop
   * @return sql statements
   */
   public Collection<String> dropTableIfPresent(ProducerCache producerCache, String tableName) {
    Table table = getTable(producerCache, tableName);
    return table == null ? Collections.emptySet() : dropTable(table);
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
        script.addAll(dialect.get().dropStatements(table));
      }
      executeScript(script);
    } finally {
      producerCache.close();
    }
    tables.get().clear();
    tablesNotNeedingTruncate.get().clear();
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
    log.debug("Dropping all views");
    try {
      Schema databaseSchema = producerCache.get().getSchema();
      ImmutableList<View> viewsToDrop = ImmutableList.copyOf(databaseSchema.views());
      List<String> script = Lists.newArrayList();
      for (View view : viewsToDrop) {
        script.addAll(dialect.get().dropStatements(view));
      }
      executeScript(script);
    } finally {
      producerCache.close();
    }
    views.get().clear();
    viewsDeployedByThis.get().clear();
  }


  /**
   * Drop all sequences.
   */
  public void dropAllSequences() {
    ProducerCache producerCache = new ProducerCache();
    log.debug("Dropping all sequences");
    try {
      Schema databaseSchema = producerCache.get().getSchema();
      ImmutableList<Sequence> sequencesToDrop = ImmutableList.copyOf(databaseSchema.sequences());
      List<String> script = Lists.newArrayList();
      for (Sequence sequence : sequencesToDrop) {
        script.addAll(dialect.get().dropStatements(sequence));
      }
      executeScript(script);
    } finally {
      producerCache.close();
    }
    sequences.get().clear();
    sequencesDeployedByThis.get().clear();
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

    if (requiredTable.getName().length() > 27) {
      log.warn("Required table name [" + requiredTable.getName() + "] is [" + requiredTable.getName().length() + "] characters long!");
    }

    // if we have an existing table, check it's identical
    Table existingTable = getTable(producerCache, requiredTable.getName());
    if (existingTable != null) {
      if (new SchemaHomology(differenceWriter, "cache", "required").tablesMatch(existingTable, requiredTable)) {
        // they match - it's identical, so we can re-use it
        dropRequired = false;
        deployRequired = false;
        if (tablesNotNeedingTruncate.get().contains(requiredTable.getName().toUpperCase())) {
          truncateRequired =  TruncationBehavior.ALWAYS.equals(truncationBehavior);
        } else {
          // if we didn't find it in the cache we don't know what state it is in, so truncate it
          truncateRequired = true;
          tablesNotNeedingTruncate.get().add(requiredTable.getName().toUpperCase());
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
   */
  private Collection<String> deployTable(Table table) {
    if (log.isDebugEnabled()) log.debug("Deploying table [" + table.getName() + "]");
    String upperCase = table.getName().toUpperCase();
    tables.get().put(upperCase, SchemaUtils.copy(table));
    tablesNotNeedingTruncate.get().add(upperCase);
    return dialect.get().tableDeploymentStatements(table);
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
    views.get().remove(view.getName().toUpperCase());
    return dialect.get().dropStatements(view);
  }


  /**
   * Deploys the specified sequence to the database.
   *
   * @param sequence the sequence to deploy
   */
  private Collection<String> deploySequence(Sequence sequence) {
    if (log.isDebugEnabled()) log.debug("Deploying sequence [" + sequence.getName() + "]");
    sequences.get().put(sequence.getName().toUpperCase(), SchemaUtils.copy(sequence));
    sequencesDeployedByThis.get().add(sequence.getName().toUpperCase());
    return dialect.get().sequenceDeploymentStatements(sequence);
  }


  /**
   * Removes the specified sequence from the database, if it exists.   Otherwise
   * do nothing.  To allow for JDBC implementations that do not support
   * conditional dropping of sequences, this will trap and ignore
   *
   * @param sequence the sequence to drop
   */
  private Collection<String> dropSequenceIfExists(Sequence sequence) {
    if (log.isDebugEnabled()) log.debug("Dropping any existing sequence [" + sequence.getName() + "]");
    sequences.get().remove(sequence.getName().toUpperCase());
    return dialect.get().dropStatements(sequence);
  }


  /**
   * Deploys the specified view to the database.
   *
   * @param view the view to deploy
   */
  private Collection<String> deployView(View view) {
    if (log.isDebugEnabled()) log.debug("Deploying view [" + view.getName() + "]");
    views.get().put(view.getName().toUpperCase(), SchemaUtils.copy(view));
    viewsDeployedByThis.get().add(view.getName().toUpperCase());
    return dialect.get().viewDeploymentStatements(view);
  }


  /**
   * Drops a table and all its dependencies (e.g. indexes).
   *
   * @param table the table to drop
   * @return sql statements
   */
  private Collection<String> dropTable(Table table) {
    if (log.isDebugEnabled()) log.debug("Dropping table [" + table.getName() + "]");
    String upperCase = table.getName().toUpperCase();
    tables.get().remove(upperCase);
    tablesNotNeedingTruncate.get().remove(upperCase);
    return dialect.get().dropStatements(table);
  }


  /**
   * Truncates the specified table.
   *
   * @param table the table to truncate.
   */
  private Collection<String> truncateTable(Table table) {
    if (log.isDebugEnabled()) log.debug("Truncating table [" + table.getName() + "]");

    // use delete-all rather than truncate, because at least on Oracle this is a lot faster when the table is small.
    return dialect.get().deleteAllFromTableStatements(table);
  }


  /**
   * Caches the {@link DatabaseDataSetProducer}, and only creates it lazily. This is required because a {@link DatabaseDataSetProducer} can be expensive to create.
   */
  private class ProducerCache {
    private DatabaseDataSetProducer producer;

    DatabaseDataSetProducer get() {
      if (producer == null) {
        producer = new DatabaseDataSetProducer(connectionResources.get(), dataSource);
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
   * Function which creates copies of sequences.
   *
   * @author Copyright (c) Alfa Financial Software 2024
   */
  private static class CopySequences implements Function<Sequence, Sequence> {
    @Override
    public Sequence apply(Sequence sequence) {
      return SchemaUtils.copy(sequence);
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
