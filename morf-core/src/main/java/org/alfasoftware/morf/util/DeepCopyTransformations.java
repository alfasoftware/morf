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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Static collection of DeepCopyTransformation utilities.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class DeepCopyTransformations {

  /**
   * Returns a DeepCopyTransformation that always delegates the copy
   * to the element.
   * @return A no-op transformation (just deep-copy)
   */
  public static DeepCopyTransformation noTransformation() {
    return new NoTransformDeepCopyTransformer();
  }


  /**
   * Convenience method to transform all items in a list.
   * @param transformables The target of the transformation
   * @param transformation The tranformation to be applied during the copy
   * @param <T> The type of transformation
   * @param <U> The type of the results of the transformation
   * @param <L> The type of iterable containing the targets
   * @return The transformations
   */
  public static <T extends DeepCopyableWithTransformation<T, ? extends U>, U extends Builder<T>, L extends Iterable<T>> ImmutableList<T> transformIterable(
      L transformables, final DeepCopyTransformation transformation) {
    return FluentIterable.from(transformables).transform(new Function<T, T>() {
      @Override
      public T apply(T input) {
        return transformation.deepCopy(input);
      }
    }).toList();
  }


  private static class NoTransformDeepCopyTransformer implements DeepCopyTransformation {

    /**
     * @see org.alfasoftware.morf.util.DeepCopyTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyableWithTransformation)
     */
    @Override
    public <T> T deepCopy(DeepCopyableWithTransformation<T,? extends Builder<T>> element) {
      return element == null ? null : element.deepCopy(this).build();
    }
  }
}