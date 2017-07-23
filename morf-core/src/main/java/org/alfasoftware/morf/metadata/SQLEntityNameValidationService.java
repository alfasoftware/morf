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

package org.alfasoftware.morf.metadata;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * <p>
 * Injectable singleton to provide validation against SQL entity names.
 * </p>
 * <p>
 * The service provides the ability to verify if a word is an SQL Reserved word
 * and if a string follows the correct naming convention for an SQL entity and
 * is within the allowed length.
 * </p>
 * <p>
 * Class uses {@link Singleton} annotation as the SQL Reserved Words are loaded
 * from a text file (in {@link SchemaValidator}) which is expensive.
 * </p>
 *
 * <p>Please see  {@link SchemaValidator} for further SQL Schema Validation.</p>
 * @author Copyright (c) Alfa Financial Software 2016
 */
@Singleton
public class SQLEntityNameValidationService {

  /**
   * Delegate work to {@link SchemaValidator} to keep code maintance in one place.
   */
  private final SchemaValidator schemaValidator;


  @Inject
  SQLEntityNameValidationService() {
    this.schemaValidator = new SchemaValidator();
  }


  /**
   * Method to establish if a given string is an SQL Reserved Word
   *
   * @param word the string to establish if its a SQL Reserved Word
   * @return true if its a SQL Reserved Word otherwise false.
   */
  public boolean isReservedWord(String word) {
    return schemaValidator.isSQLReservedWord(word);
  }


  /**
   * <p>
   * Method to establish if a name matches an allowed pattern of characters to
   * follow the correct naming convention.
   * </p>
   * <p>
   * The name must:
   * </p>
   * <ul>
   * <li>begin with an alphabetic character [a-zA-Z]</li>
   * <li>only contain alphanumeric characters or underscore [a-zA-Z0-9_]</li>
   * </ul>
   *
   * @param name The string to check if it follows the correct naming convention
   * @return true if the name is valid otherwise false
   */
  public boolean isNameConventional(String name) {
    return schemaValidator.isNameConventional(name);
  }


  /**
   * Method to establish if a given string representing an Entity name is within
   * the allowed length of charchaters.
   *
   * @see SchemaValidator#MAX_LENGTH
   * @param name the string to establish if its within the allowed length
   * @return true if its within the allowed length otherwise false.
   */
  public boolean isEntityNameLengthValid(String name) {
    return schemaValidator.isEntityNameLengthValid(name);
  }

}
