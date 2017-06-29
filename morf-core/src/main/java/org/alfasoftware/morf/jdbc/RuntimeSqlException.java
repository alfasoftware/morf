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

import java.sql.SQLException;

/**
 * Provides specific error messages involving error codes from an underlying {@link SQLException}.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class RuntimeSqlException extends RuntimeException {

  private static final long serialVersionUID = 1L;


  /**
   * Creates an SQL runtime exception based on an underlying exception and an error message to provide context.
   *
   * @param message Message that describes the processing at the time of the exception.
   * @param e Underlying exception.
   */
  public RuntimeSqlException(String message, SQLException e) {
    super(message + ": Error code [" + e.getErrorCode() + "] SQL state [" + e.getSQLState() + "]", e);
  }


  /**
   * Creates an SQL runtime exception based on an underlying exception.
   *
   * @param e Underlying exception.
   */
  public RuntimeSqlException(SQLException e) {
    super(e.getMessage() + ": Error code [" + e.getErrorCode() + "] SQL state [" + e.getSQLState() + "]", e);
  }
}
