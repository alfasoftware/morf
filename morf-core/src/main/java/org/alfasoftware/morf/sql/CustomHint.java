/* Copyright 2021 Alfa Financial Software
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
package org.alfasoftware.morf.sql;


/**
 *
 * Interface for Custom hints
 *
 * @deprecated This interface and its implementing classes should be removed in the near future as platform specific classes should be outside of core project
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
@Deprecated
public interface CustomHint extends Hint {

  /**
   * Get the string representation of this custom hint
   *
   * @return string, the string representation of this custom hint
   */
  public String getCustomHint();
}
