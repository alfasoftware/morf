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

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.Test;


/**
 * All tests of {@link DatabaseExceptionHelper}
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestDatabaseExceptionHelper {

  private final DatabaseExceptionHelper databaseExceptionHelper = new DatabaseExceptionHelper();


  /**
   * Test if detection works for throwables which have
   * {@link SQLTimeoutException} in the causes.
   */
  @Test
  public void testIsCausedByTimeoutForSQLTimeoutException() {
    // when then
    assertEquals(true, databaseExceptionHelper.isCausedByTimeoutException(new ExtendsSQLTimeoutException()));
  }


  /**
   * Test if detection works for special cases of MySQL timeout exceptions which
   * do not extend {@link SQLTimeoutException} (but only {@link SQLException}).
   */
  @Test
  public void testIsCausedByTimeoutForMySQLTimeoutException() {
    // when then
    assertEquals(true, databaseExceptionHelper.isCausedByTimeoutException(new MySQLTimeoutException()));
  }

  /**
   * Test only generic exception which extends {@link SQLTimeoutException}
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  private static class ExtendsSQLTimeoutException extends SQLTimeoutException {
  }

  /**
   * Test only exception which pretends to be one of the MySQL timeout realted
   * exception (simple name matches).
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  private static class MySQLTimeoutException extends SQLException {
  }
}
