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
 * An enumeration of the possible SQL functions that can be used in SQL
 * statements.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public enum FunctionType {

  /**
   * Count function (e.g. COUNT(*) FROM BLAH) specified
   */
  COUNT,

  /**
   * Maximum function (e.g. MAX(scheduleStartDate))
   */
  MAX,

  /**
   * Minimum function (e.g. MIN(scheduleStartDate))
   */
  MIN,

  /**
   * Some function. Find the maximum value of a boolean column (e.g. SOME(booleanField))
   */
  SOME,

  /**
   * Every function. Find the minimum value of a boolean column (e.g. EVERY(booleanField))
   */
  EVERY,

  /**
   * Sum function (e.g. SUM(assetCost))
   */
  SUM,

  /**
   * Modulo function (e.g. MOD(numberOfAssets, 10))
   */
  MOD,

  /**
   * Is null function
   */
  IS_NULL,

  /**
   * Convert YYYYMMDD to date (method subject to dialect)
   */
  YYYYMMDD_TO_DATE,


  /**
   * Convert sql date to YYYYMMDD integer
   */
  DATE_TO_YYYYMMDD,


  /**
   * Convert sql date to yyyyMMddHHmmss long
   */
  DATE_TO_YYYYMMDDHHMMSS,


  /**
   * Now function (method subject to dialect, returns the current date-time as a UTC timestamp).
   */
  NOW,


  /**
   * Substring function
   */
  SUBSTRING,

  /**
   * Coalesce function
   */
  COALESCE,

  /**
   * Calculate the number of days between two bounds, effectively
   * {@code arg1 - arg0}.
   */
  DAYS_BETWEEN,

  /**
   * Calculate the number of whole months between two dates, effectively
   * {@code in_months(arg1 - arg0)}.
   */
  MONTHS_BETWEEN,

  /**
   * Left Trim function. Trim leading spaces
   */
  LEFT_TRIM,

  /**
   * Right Trim function. Trim trailing spaces
   */
  RIGHT_TRIM,

  /**
   * Adds (or subtracts) the specified amount of days from a date
   */
  ADD_DAYS,

  /**
   * Adds (or subtracts) the specified amount of months from a date
   */
  ADD_MONTHS,

  /**
   * Round function. Rounds number to specified number of places (e.g.
   * Round(rate,2))
   */
  ROUND,

  /**
   * Floor function. Rounds number down to an integer value.
   */
  FLOOR,

  /**
   * Random function. Returns a pseudo-random number, there is no support for
   * specifying the seed. Returns a number between 0 and 1.
   */
  RANDOM,

  /**
   * Random string function. Returns a pseudo-random string of a specified
   * length. No guarantees are made on the case of the characters contained.
   */
  RANDOM_STRING,

  /**
   * Power function to raise one argument to the power of another
   */
  POWER,

  /**
   * Lower function. Lower case the argument.
   */
  LOWER,

  /**
   * Upper function. Upper case the argument.
   */
  UPPER,

  /**
   * Left Pad function. Pads the specified character on the left of specified field to stretch it to the length specified.
   */
  LEFT_PAD,

  /**
   * Length function.
   */
  LENGTH,

  /**
   * Average function.
   */
  AVERAGE,

  /**
   * Last day of the month from the date provider as an argument.
   */
  LAST_DAY_OF_MONTH
}