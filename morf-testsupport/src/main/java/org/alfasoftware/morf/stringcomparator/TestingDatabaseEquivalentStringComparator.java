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

import com.google.inject.AbstractModule;

/**
 *
 * <p>Implementation of DatabaseEquivalentStringComparator for use in tests.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestingDatabaseEquivalentStringComparator implements DatabaseEquivalentStringComparator {

  /**
   * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
   */
  @Override
  public int compare(String left, String right) {
    return left.compareTo(right);
  }

  /**
   * @see org.alfasoftware.morf.stringcomparator.DatabaseEquivalentStringComparator#compare(java.lang.Comparable, java.lang.Comparable)
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public int compare(Comparable<?> left, Comparable<?> right) {
    return ((Comparable)left).compareTo(right);
  }


  public static class Module extends AbstractModule {
    /**
     * @see com.google.inject.AbstractModule#configure()
     */
    @Override
    protected void configure() {
      binder().bind(DatabaseEquivalentStringComparator.class).to(TestingDatabaseEquivalentStringComparator.class);
    }
  }
}

