package org.alfasoftware.morf.upgrade;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.alfasoftware.morf.metadata.Index;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
}
