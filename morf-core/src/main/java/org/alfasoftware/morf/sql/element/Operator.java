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
 * An enumeration of the possible SQL operators.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public enum Operator {

  /**
   * Equals
   */
  EQ("="),

  /**
   * Not Equal To
   */
  NEQ("<>"),

  /**
   * Is Null
   */
  ISNULL("IS NULL"),

  /**
   * Is not Null
   */
  ISNOTNULL("IS NOT NULL"),

  /**
   * Greater than
   */
  GT(">"),

  /**
   * Less than
   */
  LT("<"),

  /**
   * Greater than or equal to
   */
  GTE(">="),

  /**
   * Less than or equal to
   */
  LTE("<="),

  /**
   * Logical And
   */
  AND("AND"),

  /**
   * Logical Or
   */
  OR("OR"),

  /**
   * Logical Not
   */
  NOT("NOT"),

  /**
   * Exists in statement
   */
  EXISTS("EXISTS"),

  /**
   * Like
   */
  LIKE("LIKE"),

  /**
   * In result set
   */
  IN("IN");


  private final String stringRepresentation;


  private Operator(String stringRepresentation) {
    this.stringRepresentation = stringRepresentation;
  }


  /**
   * @see java.lang.Enum#toString()
   */
  @Override
  public String toString() {
    return stringRepresentation;
  }

  /**
   * Helper method to invert an operator.
   *
   * @param operator the operator to invert
   * @return the inverted operator
   */
  public static Operator opposite(Operator operator) {
    switch(operator) {
      case EQ:
        return NEQ;
      case NEQ:
        return EQ;
      case GT:
        return LTE;
      case LT:
        return GTE;
      case GTE:
        return LT;
      case LTE:
        return GT;
      case ISNULL:
        return ISNOTNULL;
      case ISNOTNULL:
        return ISNULL;
      default:
        throw new UnsupportedOperationException("Cannot find opposite of [" + operator + "] operator");
    }
  }
}
