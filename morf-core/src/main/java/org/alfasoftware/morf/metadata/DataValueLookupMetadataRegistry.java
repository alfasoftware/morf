package org.alfasoftware.morf.metadata;

import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * Static internment registry for {@link DataValueLookupMetadata}. Reduces
 * memory usage and GC.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2019
 */
final class DataValueLookupMetadataRegistry {

  /**
   * Used for synchronisation when replacing the floor of the forest.
   */
  private static final Object SYNC = new Object();

  /**
   * The floor of the forest in our tree-based copy-on-write internment data store.
   */
  private static volatile ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> lookups = ImmutableMap.of();

  /**
   * Not constructable.
   */
  private DataValueLookupMetadataRegistry() {}


  /**
   * Interns the metadata for a single-column record, returning the same {@link DataValueLookupMetadata}
   * for all records which contain that one record.
   *
   * <p>Used when initialising a new {@link DataValueLookupBuilderImpl}.</p>
   *
   * @param columnName A case-insensitive name for the column.
   * @return The interned metadata.
   */
  static DataValueLookupMetadata intern(CaseInsensitiveString columnName) {

    ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> old = lookups;
    DataValueLookupMetadata result = old.get(columnName);
    if (result != null) {
      return result;
    }

    synchronized (SYNC) {

      ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> current = lookups;
      if (old != current) {
        result = current.get(columnName);
        if (result != null) {
          return result;
        }
      }

      result = new DataValueLookupMetadata(ImmutableList.of(columnName));
      lookups = builderPlusOneEntry(current)
          .putAll(current)
          .put(columnName, result)
          .build();
      return result;
    }
  }


  /**
   * Given an existing (interned) metadata descriptor, appends the given column and
   * returns the interned result.
   *
   * <p>Used when adding a new value to an existing {@link DataValueLookupBuilderImpl}.</p>
   *
   * <p>This call pattern means we can avoid constructing the combined {@link DataValueLookupMetadata}
   * simply to find that there is already an interned instance and throw it away. If the
   * metadata is already interned, the caller only needs to provide their current
   * metadata and the column name to add.</p>
   *
   * @param appendTo The metadata prior to appending the column.
   * @param columnName The column name to append.
   * @return The interned result.
   */
  static DataValueLookupMetadata appendAndIntern(DataValueLookupMetadata appendTo, CaseInsensitiveString columnName) {

    ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> old = appendTo.getChildren();
    DataValueLookupMetadata result = old.get(columnName);
    if (result != null) {
      return result;
    }

    synchronized (appendTo) {

      ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> current = appendTo.getChildren();
      if (old != current) {
        result = current.get(columnName);
        if (result != null) {
          return result;
        }
      }

      result = new DataValueLookupMetadata(ImmutableList.<CaseInsensitiveString>builderWithExpectedSize(appendTo.getColumnNames().size() + 1)
          .addAll(appendTo.getColumnNames())
          .add(columnName)
          .build());

      appendTo.setChildren(
          builderPlusOneEntry(current)
              .putAll(current)
              .put(columnName, result)
              .build());

      return result;
    }
  }


  /**
   * Relatively inefficient internment method for use when deserializing.
   * Interns the metadata or identifies the existing interned object and returns
   * the deuplicated result.
   *
   * @param potentialDuplicate The potential duplicate of an interned instance,
   *          to be interned and replaced
   * @return The interned and deduplicated instance.
   */
  static DataValueLookupMetadata deduplicate(DataValueLookupMetadata potentialDuplicate) {
    Iterator<CaseInsensitiveString> columnsIterator = potentialDuplicate.getColumnNames().iterator();
    DataValueLookupMetadata result = intern(columnsIterator.next());
    while (columnsIterator.hasNext()) {
      result = appendAndIntern(result, columnsIterator.next());
    }
    return result;
  }


  private static Builder<CaseInsensitiveString, DataValueLookupMetadata> builderPlusOneEntry(ImmutableMap<CaseInsensitiveString, DataValueLookupMetadata> existing) {
    return ImmutableMap.<CaseInsensitiveString, DataValueLookupMetadata>builderWithExpectedSize(existing.size() + 1);
  }
}