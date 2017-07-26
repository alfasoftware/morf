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

package org.alfasoftware.morf.stringcomparator;

import java.util.Comparator;

import org.alfasoftware.morf.stringcomparator.DatabaseEquivalentStringComparator.DatabaseEquivalentStringComparatorNoOp;

import com.google.inject.ProvidedBy;
import com.google.inject.Provider;

/**
 * API to compare two {@link String} objects according to the locale of the
 * backing database. {@link Comparable} objects are passed and if both are
 * not {@link String} objects then implementations should fall back to
 * left.compareTo(right).
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@ProvidedBy(DatabaseEquivalentStringComparatorNoOp.class)
public interface DatabaseEquivalentStringComparator extends Comparator<String> {

  /**
   * Overloaded method for API convenience. If both provided {@link Comparable}
   * objects are Strings then  compare(String, String) will be used to
   * perform a database equivalent string comparison.  Otherwise left will be
   * compared to right using the {@link Comparable#compareTo(Object)} method of left.
   *
   * @param left The left member of the comparison
   * @param right The right side of the comparison
   * @return  an int as per contract in {@link Comparator#compare(Object, Object)}
   * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
   */
  int compare(Comparable<?> left, Comparable<?> right);

  /**
   * No-op implementation.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  static final class DatabaseEquivalentStringComparatorNoOp implements Provider<DatabaseEquivalentStringComparator> {
    @Override
    public DatabaseEquivalentStringComparator get() {
      return new DatabaseEquivalentStringComparator() {
        @Override
        public int compare(String o1, String o2) {
          throw new UnsupportedOperationException("DatabaseEquivalentStringComparator disabled in this application.");
        }

        @Override
        public int compare(Comparable<?> left, Comparable<?> right) {
          throw new UnsupportedOperationException("DatabaseEquivalentStringComparator disabled in this application.");
        }
      };
    }
  }
}
