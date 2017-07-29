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

/**
 * A transformation that can intercept a deep copy operation.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public interface DeepCopyTransformation{

  /**
   * Wraps a deep copy operation in transformation which may replace instead of deep copy.
   *
   * <strong> Note that the element may be null.</strong> This is to create a cleaner API for the elements.
   * @param element The element to transform, which may be null.
   * @param <T> The type of instance built by the builder
   * @return The transformed element which <strong>must</strong> be a new object.
   */
  <T> T deepCopy(DeepCopyableWithTransformation<T,? extends Builder<T>> element);
}