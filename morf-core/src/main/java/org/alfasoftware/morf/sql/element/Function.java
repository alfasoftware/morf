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

import static org.alfasoftware.morf.sql.SqlUtils.literal;

import java.util.List;

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Months;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Function is a representation of an SQL function.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public final class Function extends AliasedField implements Driver {

  /**
   * The type of function to call
   */
  private final FunctionType             type;

  /**
   * The arguments to the function
   */
  private final ImmutableList<AliasedField> arguments;


  /**
   * Constructor used to create the deep copy of this function
   *
   * @param sourceFunction the source function to create the deep copy from
   */
  private Function(Function sourceFunction, DeepCopyTransformation transformer) {
    super(sourceFunction.getAlias());
    this.type = sourceFunction.type;
    this.arguments = FluentIterable.from(sourceFunction.arguments).transform(transformer::deepCopy).toList();
  }


  private Function(String alias, FunctionType type, ImmutableList<AliasedField> arguments) {
    super(alias);
    this.type = type;
    this.arguments = arguments;
  }


  private Function(FunctionType type, AliasedField... arguments) {
    this("", type, ImmutableList.copyOf(arguments));
  }


  private Function(FunctionType type, Iterable<? extends AliasedField> arguments) {
    this("", type, ImmutableList.copyOf(arguments));
  }


  /**
   * Gets the list of arguments associated with the function.
   *
   * @return the arguments
   */
  public List<AliasedField> getArguments() {
    return arguments;
  }


  /**
   * Helper method to create an instance of the "count" SQL function.
   *
   * @return an instance of a count function
   */
  public static Function count() {
    return new Function(FunctionType.COUNT);
  }


  /**
   * Helper method to create an instance of the "count" SQL function.
   *
   * @param field the field to evaluate in the count function.
   *
   * @return an instance of a count function
   */
  public static Function count(AliasedField field) {
    return new Function(FunctionType.COUNT, field);
  }


  /**
   * Helper method to create an instance of the "average" SQL function.
   *
   * @param field the field to evaluate in the average function.
   * @return an instance of a average function.
   */
  public static Function average(AliasedField field) {
    return new Function(FunctionType.AVERAGE, field);
  }


  /**
   * Helper method to create an instance of the "maximum" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the maximum function. This can be any expression resulting in a single column of data.
   * @return an instance of the maximum function
   */
  public static Function max(AliasedField fieldToEvaluate) {
    return new Function(FunctionType.MAX, fieldToEvaluate);
  }


  /**
   * Helper method to create an instance of the "minimum" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the minimum function. This can be any expression resulting in a single column of data.
   * @return an instance of the minimum function
   */
  public static Function min(AliasedField fieldToEvaluate) {
    return new Function(FunctionType.MIN, fieldToEvaluate);
  }


  /**
   * Helper method to create an instance of the "some" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the some function.
   * @return an instance of the some function
   */
  public static Function some(AliasedField fieldToEvaluate) {
    return new Function(FunctionType.SOME, fieldToEvaluate);
  }


  /**
   * Helper method to create an instance of the "every" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the every function.
   * @return an instance of the every function
   */
  public static Function every(AliasedField fieldToEvaluate) {
    return new Function(FunctionType.EVERY, fieldToEvaluate);
  }


  /**
   * Helper method to create an instance of the "sum" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the sum function. This can be any expression resulting in a single column of data.
   * @return an instance of the sum function
   */
  public static Function sum(AliasedField fieldToEvaluate) {
    return new Function(FunctionType.SUM, fieldToEvaluate);
  }


  /**
   * Helper method to create an instance of the "length" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the length function. This can be any expression resulting in a single column of data.
   * @return an instance of the length function.
   */
  public static Function length(AliasedField fieldToEvaluate) {
    return new Function(FunctionType.LENGTH, fieldToEvaluate);
  }


  /**
   * Helper method to create an instance of the "length-of-BLOB" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the length function. This can be any expression resulting in a single column of data.
   * @return an instance of the length function.
   */
  public static Function blobLength(AliasedField fieldToEvaluate) {
    return new Function(FunctionType.BLOB_LENGTH, fieldToEvaluate);
  }


  /**
   * Helper method to create an instance of the "YYYYMMDDToDate" SQL function.
   * {@code expression} must result in a string.
   *
   * @see Cast
   * @param expression the expression to evaluate
   * @return an instance of the YYYYMMDDToDate function
   */
  public static Function yyyymmddToDate(AliasedField expression) {
    return new Function(FunctionType.YYYYMMDD_TO_DATE, expression);
  }


  /**
   * Helper method to create an instance of the "DATE_TO_YYYYMMDD" SQL function.
   * {@code expression} must result in a string.
   *
   * @see Cast
   * @param expression the expression to evaluate
   * @return an instance of the DATE_TO_YYYYMMDD function
   */
  public static Function dateToYyyymmdd(AliasedField expression) {
    return new Function(FunctionType.DATE_TO_YYYYMMDD, expression);
  }


  /**
   * Helper method to create an instance of the "DATE_TO_YYYYMMDDHHMMSS" SQL function.
   * {@code expression} must result in a string.
   *
   * @see Cast
   * @param expression the expression to evaluate
   * @return an instance of the DATE_TO_YYYYMMDDHHMMSS function
   */
  public static Function dateToYyyyMMddHHmmss(AliasedField expression) {
    return new Function(FunctionType.DATE_TO_YYYYMMDDHHMMSS, expression);
  }


  /**
   * Helper method to create an instance of the "now" SQL function that returns the
   * current timestamp in UTC across all database platforms.
   *
   * @return an instance of a now function as a UTC timestamp.
   */
  public static Function now() {
    return new Function(FunctionType.NOW);
  }


  /**
   * Helper method to create an instance of the "substring" SQL function.
   *
   * @param expression the expression to evaluate
   * @param start the start point in the substring
   * @param length the length of the substring
   * @return an instance of the substring function
   */
  public static Function substring(AliasedField expression, AliasedField start, AliasedField length) {
    return new Function(FunctionType.SUBSTRING, expression, start, length);
  }


  /**
   * Helper method to create an instance of the "addDays" SQL function.
   *
   * @param expression the expression to evaluate
   * @param number an expression evaluating to the number of days to add (or if negative, subtract)
   * @return an instance of the addDays function
   */
  public static Function addDays(AliasedField expression, AliasedField number) {
    return new Function(FunctionType.ADD_DAYS, expression, number);
  }


  /**
   * Helper method to create an instance of the "addMonths" SQL function.
   *
   * @param expression the expression to evaluate
   * @param number an expression evaluating to the number of months to add (or if negative, subtract)
   * @return an instance of the addMonths function
   */
  public static Function addMonths(AliasedField expression, AliasedField number) {
    return new Function(FunctionType.ADD_MONTHS, expression, number);
  }


  /**
   * Rounding result for all of the below databases is equivalent to the Java RoundingMode#HALF_UP
   * for the datatypes which are currently in use at Alfa, where rounding is performed to the nearest integer
   * unless rounding 0.5 in which case the number is rounded up. Note that this is the rounding mode
   * commonly taught at school.
   *
   * <p>Example : 3.2 rounds to 3 and 3.5 rounds to 4.</p>
   *
   * <table>
   * <caption>Database rounding references</caption>
   * <tr><th>Database</th><th>Database Manual</th></tr>
   * <tr><td>Oracle</td><td>http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions135.htm</td></tr>
   * <tr><td>MySQL</td><td>http://dev.mysql.com/doc/refman/5.0/en/mathematical-functions.html#function_round</td></tr>
   * <tr><td>SQLServer</td><td>http://technet.microsoft.com/en-us/library/ms175003.aspx</td></tr>
   * <tr><td>Db2400</td><td>http://publib.boulder.ibm.com/infocenter/db2luw/v9/index.jsp?topic=%2Fcom.ibm.db2.udb.admin.doc%2Fdoc%2Fr0000845.htm</td></tr>
   * <tr><td>H2</td><td>http://www.h2database.com/html/functions.html#round</td></tr>
   * </table>
   *
   * @param expression the expression to evaluate
   * @param number an expression evaluating to the number of decimal places to round the expression to
   * @return an instance of the round function
   */
  public static Function round(AliasedField expression, AliasedField number) {
    return new Function(FunctionType.ROUND, expression, number);
  }


  /**
   * Helper method to create an instance of the "floor" SQL function, which will
   * round the provided value down to an integer value.
   * <p>
   * Example : 3.2, 3.5 and 3.9 will all round to 3
   * </p>
   *
   * @param expression the expression to evaluate
   * @return an instance of the floor function
   */
  public static Function floor(AliasedField expression) {
    return new Function(FunctionType.FLOOR, expression);
  }


  /**
   * Helper method to create an instance of the "isnull" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the is null function
   * @param replacementValue The replacement value
   * @return an instance of the is null function
   *
   * @deprecated Use {@link #coalesce(AliasedField...)} instead.
   */
  @Deprecated
  public static Function isnull(AliasedField fieldToEvaluate, AliasedField replacementValue) {
    return new Function(FunctionType.IS_NULL, fieldToEvaluate, replacementValue);
  }


  /**
   * Helper method to create an instance of the "modulo" SQL function.
   *
   * @param fieldToEvaluate the field to evaluate in the modulo function
   * @param modulus The modulus value to use
   * @return an instance of the modulo function
   */
  public static Function mod(AliasedField fieldToEvaluate, AliasedField modulus) {
    return new Function(FunctionType.MOD, fieldToEvaluate, modulus);
  }


  /**
   * Helper method to create an instance of the "coalesce" SQL function,
   * which will result in the first non-null argument.
   *
   * @param fields the fields to evaluate.
   * @return an instance of the coalesce function.
   */
  public static Function coalesce(AliasedField... fields) {
    return new Function(FunctionType.COALESCE, fields);
  }


  /**
   * Helper method to create an instance of the "coalesce" SQL function,
   * which will result in the first non-null argument.
   *
   * @param fields the fields to evaluate.
   * @return an instance of the coalesce function.
   */
  public static Function coalesce(Iterable<? extends AliasedField> fields) {
    return new Function(FunctionType.COALESCE, fields);
  }


  /**
   * The number of days between two dates including one bound, but excluding the other;
   * so {@code daysBetween(2012-12-20, 2012-12-21)} is 1.
   *
   * @param fromDate Lower bound.
   * @param toDate Upper bound.
   * @return function An instance of the "days between" function.
   */
  public static AliasedField daysBetween(AliasedField fromDate, AliasedField toDate) {
    return new Function(FunctionType.DAYS_BETWEEN, toDate, fromDate);
  }


  /**
   * The number of whole months between two dates.  The logic used is equivalent to
   * {@link Months#monthsBetween(org.joda.time.ReadableInstant, org.joda.time.ReadableInstant)}.
   *
   * <p>As an example, assuming two dates are in the same year and the {@code fromDate} is from two months prior to
   * the {@code toDate} (i.e. {@code MONTH(toDate) - MONTH(fromDate) = 2)} then:</p>
   * <ul>
   *  <li> If the {@code toDate} day of the month is greater than or equal to the {@code fromDate}
   *       day of the month, then the difference is two months;
   *  <li> If the {@code toDate} day of the month lies on the end of the month, then the difference is
   *       two months, to account for month length differences (e.g. 31 Jan &gt; 28 Feb = 1; 30 Jan &gt; 27 Feb = 0);
   *  <li> Otherwise, the difference is one (e.g. if the day of {@code fromDate} &gt; day of {@code toDate}).
   * </ul>
   *
   * @param fromDate Lower bound.
   * @param toDate Upper bound.
   * @return function An instance of the "months between" function.
   */
  public static Function monthsBetween(AliasedField fromDate, AliasedField toDate) {
    return new Function(FunctionType.MONTHS_BETWEEN, toDate, fromDate);
  }


  /**
   * Find the last day of the month from a given date.
   *
   * @param date field to evaluate containing a date
   * @return an instance of a function to find the last day of the month
   */
  public static Function lastDayOfMonth(AliasedField date) {
    return new Function(FunctionType.LAST_DAY_OF_MONTH, date);
  }


  /**
   * Helper method to create an instance of the "leftTrim" SQL function,
   * which will result in argument having leading spaces removed.
   *
   * @param expression the field to evaluate.
   * @return an instance of the leftTrim function.
   */
  public static Function leftTrim(AliasedField expression) {
    return new Function(FunctionType.LEFT_TRIM, expression);
  }


  /**
   * Helper method to create an instance of the "rightTrim" SQL function,
   * which will result in argument having trailing spaces removed.
   *
   * @param expression the field to evaluate.
   * @return an instance of the rightTrim function.
   */
  public static Function rightTrim(AliasedField expression) {
    return new Function(FunctionType.RIGHT_TRIM, expression);
  }


  /**
   * Helper method to create an instance of the "random" SQL function.
   *
   * @return an instance of the random function.
   */
  public static Function random() {
    return new Function(FunctionType.RANDOM);
  }


  /**
   * Helper method to create a function for generating random strings via SQL.
   *
   * @param length The length of the generated string
   * @return an instance of the randomString function.
   */
  public static Function randomString(AliasedField length) {
    return new Function(FunctionType.RANDOM_STRING, length);
  }


  /**
   * Helper method to create a function for raising one argument to the power of another.
   *<p>
   * Example : power(10,3) would become 1000
   *</p>
   * @param operand1 the base
   * @param operand2 the exponent
   * @return an instance of the multiply function.
   */
  public static Function power(AliasedField operand1, AliasedField operand2) {
    return new Function(FunctionType.POWER, operand1, operand2);
  }


  /**
   * Helper method to create an instance of the <code>LOWER</code> SQL function.
   * Converts all of the characters in this String to lower case using the rules
   * of the default locale.
   *
   * @param expression the expression to evaluate.
   * @return an instance of the lower function.
   */
  public static Function lowerCase(AliasedField expression) {
    return new Function(FunctionType.LOWER, expression);
  }


  /**
   * Helper method to create an instance of the <code>UPPER</code> SQL function.
   * Converts all of the characters in this String to upper case using the rules
   * of the default locale.
   *
   * @param expression the expression to evaluate.
   * @return an instance of the upper function.
   */
  public static Function upperCase(AliasedField expression) {
    return new Function(FunctionType.UPPER, expression);
  }


  /**
   * Helper method to create an instance of the <code>LPAD</code> SQL function.
   * <p>Pads the <code>character</code> on the left of <code>field</code> so that the size equals <code>length</code></p>
   * <p>The field should be of type {@link org.alfasoftware.morf.metadata.DataType#STRING}</p>
   *
   * @param field     String field to pad.
   * @param length    target length.
   * @param character character to pad.
   * @return an instance of LPAD function.
   */
  public static Function leftPad(AliasedField field, AliasedField length, AliasedField character) {
    return new Function(FunctionType.LEFT_PAD, field, length, character);
  }


  /**
   * Convenience helper method to create an instance of the <code>LPAD</code> SQL function.
   * <p>Pads the <code>character</code> on the left of <code>field</code> so that the size equals <code>length</code></p>
   * <p>The field should be of type {@link org.alfasoftware.morf.metadata.DataType#STRING}</p>
   *
   * @param field     String field to pad.
   * @param length    target length.
   * @param character character to pad.
   * @return an instance of LPAD function.
   */
  public static Function leftPad(AliasedField field, int length, String character) {
    return new Function(FunctionType.LEFT_PAD, field, literal(length), literal(character));
  }


  /**
   * Get the type of the function.
   *
   * @return the type
   */
  public FunctionType getType() {
    return type;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected Function deepCopyInternal(DeepCopyTransformation transformer) {
    return new Function(Function.this, transformer);
  }


  @Override
  protected AliasedField shallowCopy(String aliasName) {
    return new Function(aliasName, type, arguments);
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getArguments());
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (arguments == null ? 0 : arguments.hashCode());
    result = prime * result + (type == null ? 0 : type.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    Function other = (Function) obj;
    if (arguments == null) {
      if (other.arguments != null)
        return false;
    } else if (!arguments.equals(other.arguments))
      return false;
    if (type != other.type)
      return false;
    return true;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return type.toString() + "(" + StringUtils.join(arguments, ", ") + ")" + super.toString();
  }
}