package org.alfasoftware.morf.upgrade;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.alfasoftware.morf.metadata.Index;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Configuration and context bean for the {@link Upgrade} process.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2024
 */
public class UpgradeConfigAndContext {

  /**
   * Set of full names of upgrade step classes which should be executed non-parallel.
   * No other other upgrade step will be running while listed upgrade steps are being executed.
   * This impacts Graph-Based Upgrade only.
   */
  private Set<String> exclusiveExecutionSteps = Set.of();

  /**
   * A schema change adaptor to be used during upgrade.
   */
  private SchemaChangeAdaptor schemaChangeAdaptor = new SchemaChangeAdaptor.NoOp();

  /**
   * A schema change adaptor to be used during upgrade.
   */
  private SchemaChangeToSchemaAdaptor schemaChangeToSchemaAdaptor = new SchemaChangeToSchemaAdaptor.NoOp();

  /**
   * A schema auto-healer to be used before upgrade.
   */
  private SchemaAutoHealer schemaAutoHealer = new SchemaAutoHealer.NoOp();


  /**
   * A map of ignored indexes.
   */
  private Map<String, List<Index>> ignoredIndexes = Map.of();


  /**
   * Set of index names that should bypass deferred creation and be built immediately during upgrade.
   */
  private Set<String> forceImmediateIndexes = Set.of();


  /**
   * Set of index names that should be deferred even when the upgrade step uses {@code addIndex()}.
   */
  private Set<String> forceDeferredIndexes = Set.of();


  /**
   * Number of threads in the deferred index executor thread pool.
   */
  private int deferredIndexThreadPoolSize = 1;

  /**
   * Maximum number of retry attempts per deferred index operation before marking it permanently FAILED.
   */
  private int deferredIndexMaxRetries = 3;

  /**
   * Base delay in milliseconds between deferred index retry attempts.
   * Each successive retry doubles this delay (exponential backoff).
   */
  private long deferredIndexRetryBaseDelayMs = 5_000L;

  /**
   * Maximum delay in milliseconds between deferred index retry attempts.
   * The exponential backoff is capped at this value.
   */
  private long deferredIndexRetryMaxDelayMs = 300_000L;

  /**
   * Maximum time in seconds to wait for all deferred index operations to complete
   * during the pre-upgrade force-build ({@link org.alfasoftware.morf.upgrade.deferred.DeferredIndexReadinessCheck#forceBuildAllPending()}).
   * Must be strictly greater than zero.
   *
   * <p>This is distinct from the {@code timeoutSeconds} parameter on
   * {@link org.alfasoftware.morf.upgrade.deferred.DeferredIndexService#awaitCompletion(long)},
   * where zero means "wait indefinitely".</p>
   */
  private long deferredIndexForceBuildTimeoutSeconds = 28_800L;



  /**
   * @see #exclusiveExecutionSteps
   */
  public Set<String> getExclusiveExecutionSteps() {
    return exclusiveExecutionSteps;
  }


  /**
   * @see #exclusiveExecutionSteps
   */
  public UpgradeConfigAndContext setExclusiveExecutionSteps(Set<String> exclusiveExecutionSteps) {
    this.exclusiveExecutionSteps = exclusiveExecutionSteps;
    return this;
  }


  /**
   * @see #schemaChangeAdaptor
   */
  public SchemaChangeAdaptor getSchemaChangeAdaptor() {
    return schemaChangeAdaptor;
  }


  /**
   * @see #schemaChangeAdaptor
   */
  public void setSchemaChangeAdaptor(SchemaChangeAdaptor schemaChangeAdaptor) {
    Preconditions.checkNotNull(schemaChangeAdaptor);
    this.schemaChangeAdaptor = schemaChangeAdaptor;
  }


  /**
   * @see #schemaChangeToSchemaAdaptor
   */
  public SchemaChangeToSchemaAdaptor getSchemaChangeToSchemaAdaptor() {
    return schemaChangeToSchemaAdaptor;
  }


  /**
   * @see #schemaChangeToSchemaAdaptor
   */
  public void setSchemaChangeToSchemaAdaptor(SchemaChangeToSchemaAdaptor schemaChangeToSchemaAdaptor) {
    Preconditions.checkNotNull(schemaChangeToSchemaAdaptor);
    this.schemaChangeToSchemaAdaptor = schemaChangeToSchemaAdaptor;
  }


  /**
   * @see #schemaAutoHealer
   */
  public SchemaAutoHealer getSchemaAutoHealer() {
    return schemaAutoHealer;
  }

  /**
   * @see #schemaAutoHealer
   */
  public void setSchemaAutoHealer(SchemaAutoHealer schemaAutoHealer) {
    Preconditions.checkNotNull(schemaAutoHealer);
    this.schemaAutoHealer = schemaAutoHealer;
  }

  /**
   * @see #ignoredIndexes
   * @return ignoredIndexes map
   */
  public Map<String, List<Index>> getIgnoredIndexes() {
    return ignoredIndexes;
  }

  /**
   * @see #ignoredIndexes
   */
  public void setIgnoredIndexes(Map<String, List<Index>> ignoredIndexes) {
    this.ignoredIndexes = ignoredIndexes.entrySet().stream()
      .collect(ImmutableMap.toImmutableMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue));
  }

  /**
   * Get the ignored indexes for table. If none return empty list.
   * @param tableName the table name
   * @return the list of ignored indexes
   */
  public List<Index> getIgnoredIndexesForTable(String tableName) {
    String toLowerCase = tableName.toLowerCase();
    if (ignoredIndexes.containsKey(toLowerCase)) {
      return ignoredIndexes.get(toLowerCase);
    } else {
      return List.of();
    }
  }


  /**
   * @see #forceImmediateIndexes
   * @return forceImmediateIndexes set
   */
  public Set<String> getForceImmediateIndexes() {
    return forceImmediateIndexes;
  }


  /**
   * @see #forceImmediateIndexes
   */
  public void setForceImmediateIndexes(Set<String> forceImmediateIndexes) {
    this.forceImmediateIndexes = forceImmediateIndexes.stream()
      .map(String::toLowerCase)
      .collect(ImmutableSet.toImmutableSet());
    validateNoIndexConflict();
  }


  /**
   * Check whether the given index name should be forced to build immediately
   * during upgrade, bypassing deferred creation.
   *
   * @param indexName the index name to check
   * @return true if the index should be built immediately
   */
  public boolean isForceImmediateIndex(String indexName) {
    return forceImmediateIndexes.contains(indexName.toLowerCase());
  }


  /**
   * @see #forceDeferredIndexes
   * @return forceDeferredIndexes set
   */
  public Set<String> getForceDeferredIndexes() {
    return forceDeferredIndexes;
  }


  /**
   * @see #forceDeferredIndexes
   */
  public void setForceDeferredIndexes(Set<String> forceDeferredIndexes) {
    this.forceDeferredIndexes = forceDeferredIndexes.stream()
      .map(String::toLowerCase)
      .collect(ImmutableSet.toImmutableSet());
    validateNoIndexConflict();
  }


  /**
   * Check whether the given index name should be forced to defer during upgrade,
   * even when the upgrade step uses {@code addIndex()}.
   *
   * @param indexName the index name to check
   * @return true if the index should be deferred
   */
  public boolean isForceDeferredIndex(String indexName) {
    return forceDeferredIndexes.contains(indexName.toLowerCase());
  }



  /**
   * @see #deferredIndexThreadPoolSize
   */
  public int getDeferredIndexThreadPoolSize() {
    return deferredIndexThreadPoolSize;
  }


  /**
   * @see #deferredIndexThreadPoolSize
   */
  public void setDeferredIndexThreadPoolSize(int deferredIndexThreadPoolSize) {
    this.deferredIndexThreadPoolSize = deferredIndexThreadPoolSize;
  }


  /**
   * @see #deferredIndexMaxRetries
   */
  public int getDeferredIndexMaxRetries() {
    return deferredIndexMaxRetries;
  }


  /**
   * @see #deferredIndexMaxRetries
   */
  public void setDeferredIndexMaxRetries(int deferredIndexMaxRetries) {
    this.deferredIndexMaxRetries = deferredIndexMaxRetries;
  }


  /**
   * @see #deferredIndexRetryBaseDelayMs
   */
  public long getDeferredIndexRetryBaseDelayMs() {
    return deferredIndexRetryBaseDelayMs;
  }


  /**
   * @see #deferredIndexRetryBaseDelayMs
   */
  public void setDeferredIndexRetryBaseDelayMs(long deferredIndexRetryBaseDelayMs) {
    this.deferredIndexRetryBaseDelayMs = deferredIndexRetryBaseDelayMs;
  }


  /**
   * @see #deferredIndexRetryMaxDelayMs
   */
  public long getDeferredIndexRetryMaxDelayMs() {
    return deferredIndexRetryMaxDelayMs;
  }


  /**
   * @see #deferredIndexRetryMaxDelayMs
   */
  public void setDeferredIndexRetryMaxDelayMs(long deferredIndexRetryMaxDelayMs) {
    this.deferredIndexRetryMaxDelayMs = deferredIndexRetryMaxDelayMs;
  }


  /**
   * @see #deferredIndexForceBuildTimeoutSeconds
   */
  public long getDeferredIndexForceBuildTimeoutSeconds() {
    return deferredIndexForceBuildTimeoutSeconds;
  }


  /**
   * @see #deferredIndexForceBuildTimeoutSeconds
   */
  public void setDeferredIndexForceBuildTimeoutSeconds(long deferredIndexForceBuildTimeoutSeconds) {
    this.deferredIndexForceBuildTimeoutSeconds = deferredIndexForceBuildTimeoutSeconds;
  }


  private void validateNoIndexConflict() {
    Set<String> overlap = Sets.intersection(forceImmediateIndexes, forceDeferredIndexes);
    if (!overlap.isEmpty()) {
      throw new IllegalStateException(
        "Index names cannot be both force-immediate and force-deferred: " + overlap);
    }
  }
}
