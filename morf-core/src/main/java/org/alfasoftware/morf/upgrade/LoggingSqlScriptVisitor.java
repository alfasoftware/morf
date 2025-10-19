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

import org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Use to log notifications received about SQL execution.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class LoggingSqlScriptVisitor implements SqlScriptVisitor {
  private final Log log = LogFactory.getLog(LoggingSqlScriptVisitor.class);

  private final String schemaPosition;

  public LoggingSqlScriptVisitor() {
    super();
    this.schemaPosition = "";
  }

  /**
   * Facilitates logging with additional info about the currently upgraded schema position.
   * @param schemaPosition String containing schema position name.
   */
  public LoggingSqlScriptVisitor(String schemaPosition) {
    super();
    this.schemaPosition = schemaPosition;
  }

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionStart()
   */
  @Override
  public void executionStart() {
    log.info(logSchemaPositionPrefix() + "Starting SQL Script");
  }

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#beforeExecute(java.lang.String)
   */
  @Override
  public void beforeExecute(String sql) {
    log.info(logSchemaPositionPrefix() + "Executing [" + sql + "]");
  }

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#afterExecute(String, long, long)
   */
  @Override
  public void afterExecute(String sql, long numberOfRowsUpdated, long durationInSeconds) {
    log.info(logSchemaPositionPrefix() + "Completed [" + sql + "] in [" + durationInSeconds + "] seconds with [" + numberOfRowsUpdated + "] rows updated");
  }

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionEnd()
   */
  @Override
  public void executionEnd() {
    log.info(logSchemaPositionPrefix() + "SQL Script Complete");
  }


  private String logSchemaPositionPrefix() {
    return StringUtils.isBlank(schemaPosition) ? "" : "Schema position[" + schemaPosition + "] ";
  }
}