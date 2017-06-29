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

package org.alfasoftware.morf.diagnostics;

import java.sql.SQLException;

import org.alfasoftware.morf.jdbc.AbstractConnectionResources;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.h2.tools.Server;

import com.google.inject.Inject;

/**
 * H2 implementation of {@linkplain DatabaseInspector}. Opens a webserver
 * which can query the contents of the database.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class H2DatabaseInspector implements DatabaseInspector {

  private static final Log log = LogFactory.getLog(H2DatabaseInspector.class);

  /** Connection information. */
  private final ConnectionResources connectionResources;


  /**
   * Inject connection configuration.
   *
   * @param connectionResources connection information.
   */
  @Inject
  public H2DatabaseInspector(ConnectionResources connectionResources) {
    super();
    this.connectionResources = connectionResources;
  }


  /**
   * @see org.alfasoftware.morf.diagnostics.DatabaseInspector#inspect()
   */
  @Override
  public void inspect() {
    if (connectionResources instanceof AbstractConnectionResources &&
        "H2".equals(connectionResources.getDatabaseType())) {
      try {
        log.info("Launching H2 database inspector...");
        Server.startWebServer(connectionResources.getDataSource().getConnection());
      } catch (SQLException e) {
        throw new IllegalStateException("Failed to start the H2 Database Inspector web server", e);
      }
    }
  }
}
