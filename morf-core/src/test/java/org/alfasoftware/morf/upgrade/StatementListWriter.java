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

package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Stores SQL statements in a list for testing.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class StatementListWriter implements SqlStatementWriter {

  /**
   * Received statements.
   */
  private final List<String> statements = new ArrayList<String>();

  /**
   * @see org.alfasoftware.morf.upgrade.SqlStatementWriter#writeSql(Collection)
   */
  @Override
  public void writeSql(Collection<String> sql) {
    statements.addAll(sql);
  }

  /**
   * @return the statements
   */
  public List<String> getStatements() {
    return statements;
  }
}
