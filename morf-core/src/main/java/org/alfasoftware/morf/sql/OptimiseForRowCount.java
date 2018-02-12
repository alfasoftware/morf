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

package org.alfasoftware.morf.sql;


/**
 * Represents the {@link SelectStatement#optimiseForRowCount(int)} hint.
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
public final class OptimiseForRowCount implements Hint {

  private final int rowCount;

  OptimiseForRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  /**
   * @return the number of rows for which to optimise.
   */
  public int getRowCount() {
    return rowCount;
  }

  @Override
  public String toString() {
    return "OptimiseForRowCount [rowCount=" + rowCount + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + rowCount;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    OptimiseForRowCount other = (OptimiseForRowCount) obj;
    if (rowCount != other.rowCount)
      return false;
    return true;
  }
}