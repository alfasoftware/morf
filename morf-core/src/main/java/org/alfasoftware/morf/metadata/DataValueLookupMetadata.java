package org.alfasoftware.morf.metadata;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Cached fast lookup allowing {@link DataValueLookupBuilderImpl} to record the position
 * of column names in its internal array and then retrieve them by column name.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2019
 */
final class DataValueLookupMetadata implements Serializable {

  private static final Log log = LogFactory.getLog(DataValueLookupMetadata.class);

  private static final long serialVersionUID = -1238257923874987123L;

  private final ImmutableList<CaseInsensitiveString> keys;

  // transient data: deserialized instances only exist temporarily before they are deduplicated, so
  // we assume we don't need to serialize this data - we'll rebuild it when resolving after
  // deserialization
  @Nullable // if size <= USE_MAP_FOR_COUNT_HIGHER_THAN
  private final transient ImmutableMap<CaseInsensitiveString, Integer> lookups;
  private transient volatile ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> children = ImmutableMap.of();
  private transient volatile int hash;

  DataValueLookupMetadata(ImmutableList<CaseInsensitiveString> keys) {
    super();

    if (log.isDebugEnabled()) log.debug("New metadata entry: " + keys);

    this.keys = keys;

    ImmutableMap.Builder<CaseInsensitiveString, Integer> builder = ImmutableMap.builder();
    Iterator<CaseInsensitiveString> iterator = keys.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      builder.put(iterator.next(), i++);
    }
    this.lookups = builder.build();
  }


  /**
   * When deserializing, resolve via the static factory. This prevents us getting duplicate
   * instances.
   *
   * @return The interned instance.
   */
  private Object readResolve()  {
    return DataValueLookupMetadataRegistry.deduplicate(this);
  }


  /**
   * Given the following record arrangements:
   *
   *  [A]
   *  [A, B]
   *  [A, B, C]
   *  [A, D, E]
   *  [A, D, F]
   *
   * We arrange our metadata in a tree:
   *
   *  [A]
   *   - [+ B]
   *     - [+ C]
   *   - [+ D]
   *     - [+ E]
   *     - [+ F]
   *
   * This method returns the children of this record arrangement in that tree.
   * It is volatile and changes as internment occurs (see {@link #setChildren(ImmutableMap)}).
   *
   * @return
   */
  ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> getChildren() {
    return children;
  }


  /**
   * Updates the children during internment.
   *
   * @param children The new map of child arrangements.
   */
  void setChildren(ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> children) {
    this.children = children;
  }


  /**
   * @return The column names stored, where the array index corresponds
   * with the {@link DataValueLookupBuilderImpl} internal array position.
   */
  List<CaseInsensitiveString> getColumnNames() {
    return keys;
  }


  /**
   * Get the array position of the specified column.
   *
   * @param columnName The column name.
   * @return The array position, or null if not present.
   */
  @Nullable
  Integer getIndexInArray(CaseInsensitiveString columnName) {
    return lookups.get(columnName);
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "DataValueLookupMetadata [columnNames=" + keys + "]";
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int h = hash;
    if (h != 0) {
      return h;
    }
    final int prime = 31;
    h = 1;
    h = prime * h + (keys == null ? 0 : keys.hashCode());
    hash = h;
    return h;
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    return this == obj; // Fully interned
  }
}