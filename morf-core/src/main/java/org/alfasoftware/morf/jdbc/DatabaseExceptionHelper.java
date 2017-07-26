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

import java.sql.SQLTimeoutException;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.inject.Singleton;

/**
 * <p>Helper which provides methods for database related {@link Throwable}
 * analysis. </p>
 * Stateless singleton service class.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@Singleton
public class DatabaseExceptionHelper {

  /**
   * Common name (different packages!) of the all known MySQL exception classes
   * which is thrown on timeout.
   */
  static final String MYSQL_TIMEOUT_EXCEPTION_NAME = "MySQLTimeoutException";


  /**
   * <p>Checks if the throwable was caused by timeout exception.</p>
   * <b>This method has been tested for Oracle and MySQL only and might not work
   * for other DB engines.</b>
   *
   * @param throwable to check
   * @return true if the throwable is caused by a timeout, false otherwise
   */
  public boolean isCausedByTimeoutException(Throwable throwable) {
    // Valid test for Oracle timeout exception and some (not all!) MySQL
    // exceptions.
    if (ExceptionUtils.indexOfType(throwable, SQLTimeoutException.class) != -1) {
      return true;
    }
    // MySQL database has two timeout exceptions in two packages. One of them
    // doesn't extend SQLTimeoutException but only SQLException. It is therefore
    // necessary to do ugly name check...
    for (Throwable causeThrowable : ExceptionUtils.getThrowables(throwable)) {
      if (MYSQL_TIMEOUT_EXCEPTION_NAME.equals(causeThrowable.getClass().getSimpleName())) {
        return true;
      }
    }
    return false;
  }
}
