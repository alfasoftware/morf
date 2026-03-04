/* Copyright 2026 Alfa Financial Software
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

package org.alfasoftware.morf.upgrade.deferred;

import com.google.inject.ImplementedBy;

/**
 * Pre-upgrade check that ensures no deferred index operations are left
 * {@link DeferredIndexStatus#PENDING} before a new upgrade run begins.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexValidatorImpl.class)
interface DeferredIndexValidator {

  /**
   * Verifies that no {@link DeferredIndexStatus#PENDING} operations exist. If
   * any are found, executes them immediately (blocking the caller) before
   * returning.
   *
   * @throws IllegalStateException if any operations failed permanently.
   */
  void validateNoPendingOperations();
}
