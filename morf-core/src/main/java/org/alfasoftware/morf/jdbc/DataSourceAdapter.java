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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;


/**
 * Base class for {@link DataSource} implementation which offer only
 * {@link Connection}
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public abstract class DataSourceAdapter implements DataSource {

  /**
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Wrappers not supported");
  }


  /**
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Wrappers not supported");
  }


  /**
   * @see javax.sql.CommonDataSource#setLoginTimeout(int)
   */
  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    throw new UnsupportedOperationException("Login timeout not supported");
  }


  /**
   * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
   */
  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    throw new UnsupportedOperationException("Log writer not supported");
  }


  /**
   * @see javax.sql.CommonDataSource#getLoginTimeout()
   */
  @Override
  public int getLoginTimeout() throws SQLException {
    throw new UnsupportedOperationException("Login timeout not supported");
  }


  /**
   * @see javax.sql.CommonDataSource#getLogWriter()
   */
  @Override
  public PrintWriter getLogWriter() throws SQLException {
    throw new UnsupportedOperationException("Log writer not supported");
  }


  /**
   * @see javax.sql.CommonDataSource#getParentLogger()
   */
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new UnsupportedOperationException("Log writer not supported by ConnectionDetails");
  }
}
