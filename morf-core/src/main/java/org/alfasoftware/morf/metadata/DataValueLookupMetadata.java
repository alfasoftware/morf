package org.alfasoftware.morf.metadata;

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
final class DataValueLookupMetadata {

  private static final Log log = LogFactory.getLog(DataValueLookupMetadata.class);

  private final ImmutableList<CaseInsensitiveString> keys;
  private final ImmutableMap<CaseInsensitiveString, Integer> lookups;

  private volatile ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> children = ImmutableMap.of();

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
}