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

import static org.alfasoftware.morf.sql.SqlUtils.caseStatement;
import static org.alfasoftware.morf.sql.SqlUtils.cast;
import static org.alfasoftware.morf.sql.SqlUtils.nullLiteralIfZero;
import static org.alfasoftware.morf.sql.SqlUtils.when;
import static org.alfasoftware.morf.sql.element.Criterion.isNull;
import static org.alfasoftware.morf.sql.element.Criterion.or;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldLiteral;

/**
 * Utility methods for manipulating DATE in SQL.
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
public class SqlDateUtils {


  /**
   * Returns the field as a date.
   *
   * @param field the field to be casted as date
   * @return the field as a date
   */
  public static AliasedField castAsDate(AliasedField field) {
    return cast(field).asString(8).asDate();
  }


  /**
   * Returns null if the supplied expression is zero, otherwise returns the expression casted to a date.
   *
   * @param expression the expression to evaluate
   * @return null or cast expression as a date
   */
  public static AliasedField castAsDateNullIfZero(AliasedField expression) {
    return castAsDate(nullLiteralIfZero(expression));
  }


  /**
   * Returns the replacement value if the evaluating expression is zero,
   * otherwise returns the value casted as date.
   *
   * @param expression the expression to evaluate
   * @param replace the replacement value
   * @return expression or replacement value casted as date
   */
  public static AliasedField castAsDateReplaceValueIfZero(AliasedField expression, AliasedField replace) {
    return castAsDateCaseStatement(expression, expression.eq(0), replace);
  }


  /**
   * Returns the replacement value if the evaluating expression is null or zero,
   * otherwise returns the value casted as date.
   *
   * @param expression the expression to evaluate
   * @param replace the replacement value
   * @return expression or replacement value casted as date
   */
  public static AliasedField castAsDateReplaceValueIfNullOrZero(AliasedField expression, AliasedField replace) {
    return castAsDateCaseStatement(expression, or(isNull(expression), expression.eq(0)), replace);
  }


  /**
   * Return the replacement value(casted as date) if the criterion is true,
   * otherwise returns the value casted as date.
   *
   * @param expression the field to be casted as DATE
   * @param criterion the criterion to evaluate
   * @param replace the replacement value
   * @return expression or replacement value casted as date
   */
  private static AliasedField castAsDateCaseStatement(AliasedField expression, Criterion criterion, AliasedField replace) {
    if (replace instanceof FieldLiteral &&
        ((FieldLiteral)replace).getDataType() == DataType.DATE) {
      return caseStatement(when(criterion).then(replace)).otherwise(castAsDate(expression));
    }
    else {
      return castAsDate(caseStatement(when(criterion).then(replace)).otherwise(expression));
    }
  }
}