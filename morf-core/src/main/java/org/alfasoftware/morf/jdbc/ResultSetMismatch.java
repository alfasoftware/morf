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

package org.alfasoftware.morf.jdbc;

import java.sql.ResultSet;
import java.util.Arrays;

/**
 * Store mismatch results after comparison of JDBC {@link ResultSet} objects.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class ResultSetMismatch {

  /**
   * Reconciliation mismatch type.
   *
   * @author Copyright (c) Alfa Financial Software 2014
   *
   */
  public enum MismatchType {
    /** A key is missing from the left hand {@link ResultSet}. */
    MISSING_LEFT,
    /** A key is missing from the right hand {@link ResultSet}. */
    MISSING_RIGHT,
    /** A record with the same key from the left hand and right hand {@link ResultSet}s mismatches on other values. */
    MISMATCH
  }

  /**
   * A key identifying the mismatch. E.g. The mismatching agreement number.
   */
  private final String[] key;

  /**
   * Mismatch type.
   */
  private final MismatchType mismatchType;

  /**
   * Mismatch column index.
   */
  private final int mismatchColumnIndex;

  /**
   * Value from the left hand result set.
   */
  private final String leftValue;

  /**
   * Value from the right hand result set.
   */
  private final String rightValue;

  /**
   * Constructor for mismatch record.
   *
   * @param mismatchType The mismatch type.
   * @param mismatchColumnIndex The column index where mismatch occurred.
   * @param leftValue The value from the left hand result set.
   * @param rightValue The value from the right hand result set.
   * @param key The key identifying the mismatch. E.g. The mismatching agreement number.
   *
   */
  ResultSetMismatch(MismatchType mismatchType, int mismatchColumnIndex, String leftValue, String rightValue, String... key) {
    super();
    this.mismatchType = mismatchType;
    this.mismatchColumnIndex = mismatchColumnIndex;
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.key = Arrays.copyOf(key, key.length);
  }


  /**
   * @return key identifying the mismatch.
   */
  public String[] getKey() {
    return Arrays.copyOf(key, key.length);
  }


  /**
   * @return Mismatch type.
   */
  public MismatchType getMismatchType() {
    return mismatchType;
  }


  /**
   * @return Mismatch column index.
   */
  public int getMismatchColumnIndex() {
    return mismatchColumnIndex;
  }


  /**
   * @return Value from the left hand result set.
   */
  public String getLeftValue() {
    return leftValue;
  }


  /**
   * @return Value from the right hand result set.
   */
  public String getRightValue() {
    return rightValue;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString(){
    StringBuilder mismatchDetails = new StringBuilder();
    if (key.length > 0) {
      mismatchDetails.append("Row key:").append(Arrays.toString(key)).append(" ");
    }
    mismatchDetails.append("Type:[").append(mismatchType)
                   .append("] Column:[").append(mismatchColumnIndex);
    mismatchDetails.append("] Values:[").append(leftValue)
                   .append(" <> ").append(rightValue).append("]");
    return mismatchDetails.toString();
  }
}
