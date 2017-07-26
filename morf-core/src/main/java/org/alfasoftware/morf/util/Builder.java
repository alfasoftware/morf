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

package org.alfasoftware.morf.util;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Generic builder interface
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public interface Builder<T> {

  /**
   * @return a constructed instance.
   */
  T build();

  /**
   * Collection of static utility methods related to the Builder interface.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  public final class Helper {

    /**
     * Function to call build() on all Builders.
     *
     * @return a Function to be used with FluentIterables.transform()
     * @param <T> The type of instance built by the builders
     */
    public static <T> com.google.common.base.Function<Builder<? extends  T>,T> buildAll() {
      return new com.google.common.base.Function<Builder<? extends T>, T>() {
        @Override
        public T apply(Builder<? extends  T> builder) {
          return builder.build();
        }
      };
    }


    /**
     * Convenience method to call build() on a list of Builders.
     *
     * @param builders a list of AliasedFieldBuilder
     * @return the result of calling build() on each builder
     * @param <T> The type of instance built by the builders
     */
    public static <T> ImmutableList<T> buildAll(Iterable<? extends Builder<? extends T>> builders) {
      return FluentIterable.from(builders)
          .transform(Helper.<T>buildAll()).toList();
    }
  }
}
