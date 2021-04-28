package org.alfasoftware.morf.metadata;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

/**
 * An efficient wrapper for a {@link String} which makes its {@link #equals(Object)}
 * and {@link #hashCode()} methods case-insensitive, for use in maps and sets.
 *
 * <p>These objects are guaranteed globally unique and therefore their hashcode
 * and equals operations are simple reference comparisons.  However, avoid using
 * these for large or potentially rare strings as they are never garbage collected.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2019
 */
public final class CaseInsensitiveString implements Serializable {

  private static final Logger log = LogManager.getLogger(CaseInsensitiveString.class);

  private static final long serialVersionUID = -928749237982340192L;

  private static final Object SYNC = new Object();

  private static volatile ImmutableMap<String, CaseInsensitiveString> cache = ImmutableMap.of();

  private final String string;
  private final transient int hash; // transient: deserialized instances only exist temporarily before they are deduplicated

  /**
   * Returns the string, wrapped as case insensitive.
   *
   * @param string The string.
   * @return The wrapped instance;
   */
  static CaseInsensitiveString of(String string) {

    // Fast case - return the existing interned instance
    CaseInsensitiveString result = cache.get(string);

    if (result == null) {
      synchronized (SYNC) {

        String asUpperCase = string.toUpperCase();
        result = cache.get(asUpperCase);
        boolean isUpperCase = asUpperCase.equals(string);

        if (result == null) {
          result = new CaseInsensitiveString(string, asUpperCase.hashCode());
          if (log.isDebugEnabled()) log.debug("New interned case insensitive string: " + result);
          if (isUpperCase) {
            internOnly(string, result);
          } else {
            internNormalAndUpperCase(string, asUpperCase, result);
          }
        } else if (!isUpperCase && !cache.containsKey(string)) {
          internOnly(string, result);
        }
      }
    }

    return result;
  }


  /**
   * Do not under any circumstances use this outside a strictly single-threaded test.
   * Clearing the cache will cause new instances to be created for previous strings.
   * If that occurs, the shortcut equals logic we use will no longer hold true.
   *
   * <p>If it becomes necessary to flush the cache at runtime to avoid memory issues,
   * a more sophisticated mechanism will be needed; e.g. temporarily moving the cache
   * to a weak collection, allowing unused objects to get GCed.</p>
   *
   * @deprecated Do not use outside single threaded tests.
   */
  @VisibleForTesting
  @Deprecated
  static void unsafeClearCache() {
    cache = ImmutableMap.of();
  }


  private static void internNormalAndUpperCase(String string, String asUpperCase, CaseInsensitiveString result) {
    cache = ImmutableMap.<String, CaseInsensitiveString>builderWithExpectedSize(cache.size() + 2)
        .putAll(cache)
        .put(string, result)
        .put(asUpperCase, result)
        .build();
  }


  private static void internOnly(String string, CaseInsensitiveString result) {
    cache = ImmutableMap.<String, CaseInsensitiveString>builderWithExpectedSize(cache.size() + 1)
        .putAll(cache)
        .put(string, result)
        .build();
  }


  /**
   * When deserializing, resolve via the static factory. This prevents us getting duplicate
   * instances.
   *
   * @return The interned instance.
   */
  private Object readResolve()  {
    return of(string);
  }


  private CaseInsensitiveString(String string, int hash) {
    this.string = string;
    this.hash = hash;
  }


  /**
   * Case-insensitive hashcode implementation.
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return hash;
  }


  /**
   * Direct instance comparison (we ensure that instances never get duplicated).
   * Use {@link #equalsString(String)} to compare with strings.
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }


  /**
   * Returns the internal representation of the string value. Note that the
   * case of this is unpredictable. Do not use to compare to other strings;
   * instead use {@link #equalsString(String)}.
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return string;
  }


  /**
   * Returns true if the passed string matches this one
   * case-insensitively.
   *
   * @param other The string to which to compare.
   * @return true if equal.
   */
  public boolean equalsString(String other) {
    return this.string.equalsIgnoreCase(other);
  }
}