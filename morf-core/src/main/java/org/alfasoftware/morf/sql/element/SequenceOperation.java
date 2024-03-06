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

package org.alfasoftware.morf.sql.element;

/**
 * Enum to define the list of operations available for types of operations available for a {@link SequenceReference}s
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public enum SequenceOperation {

  /**
   * Operation to request the next value for a sequence.
   */
  NEXT_VALUE,

  /**
   * Operation to request the current value for a sequence.
   */
  CURRENT_VALUE

}
