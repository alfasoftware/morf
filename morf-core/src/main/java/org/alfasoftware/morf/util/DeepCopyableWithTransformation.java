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
 * Declares that an object can be copied to create a complete copy of the object where
 * the copy transformation is provided by {@link DeepCopyTransformation}. This call should be
 * propagated to child-objects.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 * @param <T> The input and output type.
 * @param <U> The resulting builder type
 */
public interface DeepCopyableWithTransformation<T, U extends Builder<T>>
{

  /**
   * Deep copies this object and all child objects. The deep copy operation can be intercepted by the transformation
   * to re-write the tree during the copy.
   *
   * @param transformation A transformation that can intercept the deep copy operations on child objects.
   * @return a builder for the copied instance.
   */
   U deepCopy(DeepCopyTransformation transformation);
}