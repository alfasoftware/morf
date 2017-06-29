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
 * Defines the operator type for some maths.
 * 
 * @author Copyright (c) Alfa Financial Software 2011
 */
public enum MathsOperator {

  /**
   * Plus.
   */
  PLUS("+"),

  /**
   * Minus.
   */
  MINUS("-"),

  /**
   * Multiply.
   */
  MULTIPLY("*"),

  /**
   * Divide.
   */
  DIVIDE("/");


  /**
   * The string representation of the operator.
   */
  private final String operator;


  /**
   * Constructor.
   * 
   * @param operator the string representation of the operator.
   */
  private MathsOperator(String operator) {
    this.operator = operator;
  }


  /**
   * @see java.lang.Enum#toString()
   */
  @Override
  public String toString() {
    return operator;
  }
}
