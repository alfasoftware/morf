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


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.google.common.io.Resources;


/**
 * Simple bean implementation of {@link ConnectionResources}.
 *
 * <p>May be initialised in the following ways:</p>
 *
 * <ul>
 *  <li>Use the no-arg constructor and then the setter methods.</li>
 *  <li>Create a {@link JdbcUrlElements} and pass that to the constructor.</li>
 *  <li>Create a properties file or stream containing the values below and pass it to the constructor.</li>
 * </ul>
 *
 * <p>Features the following configurable properties:</p>
 *
 * <dl>
 *  <dt>databaseType</dt><dd>The database type to connect to. See {@link DatabaseType#identifier()}.</dd>
 *  <dt>hostName</dt><dd>The server host name. Defaults to localhost.</dd>
 *  <dt>port</dt><dd>Port (or zero for default).</dd>
 *  <dt>instanceName</dt><dd>Several RDBMS allow you to run several instances of the database
 *                           server software on the same machine all behind the same network connection
 *                           details. This identifies the instance of the database server on the specified host.
 *                           <ul>
 *                            <li>On Oracle, this is the SID and it is mandatory.</li>
 *                            <li>On SQL Server, this is the instance name and can be blank for the default instance.</li>
 *                            <li>On other databases this is ignored.</li>
 *                           </ul>
 *                       </dd>
 *  <dt>databaseName</dt><dd>For RDBMS where there might be several databases within an instance of
 *                           the database server.
 *                           <ul>
 *                            <li>On SQL Server this is the "database".</li>
 *                            <li>On MySQL, this is the "database".</li>
 *                            <li>On Oracle this is not required - the next level within an instance (SID) is a schemaName.</li>
 *                           </ul>
 *                       </dd>
 *  <dt>schemaName</dt><dd>For RDBMS where there might be several database schemas within a database.
 *                         <ul>
 *                          <li>On Oracle this is the "schema".</li>
 *                          <li>On SQL Server this is the "".</li>
 *                          <li>On MySQL, this is not required.</li>
 *                         </ul>
 *                     </dd>
 *  <dt>userName</dt><dd>The JDBC username.</dd>
 *  <dt>password</dt><dd>The JDBC password.</dd>
 *  <dt>statementPoolingMaxStatements</dt><dd>The maximum number of statements to cache in a statement pool.</dd>
 * </dl>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class ConnectionResourcesBean extends AbstractConnectionResources {

  private String databaseType;
  private String hostName;
  private int port;
  private String instanceName;
  private String databaseName;
  private String schemaName;
  private String userName;
  private int statementPoolingMaxStatements;
  private transient String password;


  /**
   * Default constructor.  Defaults values from system properties.
   */
  public ConnectionResourcesBean() {
    super();
  }


  /**
   * Construct from a {@link JdbcUrlElements}.
   *
   * @param e The URL elements.
   */
  public ConnectionResourcesBean(JdbcUrlElements e) {
    super();
    this.databaseType = e.getDatabaseType();
    this.hostName = e.getHostName();
    this.port = e.getPort();
    this.databaseName = e.getDatabaseName();
    this.instanceName = e.getInstanceName();
    this.schemaName = e.getSchemaName();
  }


  /**
   * Construct from {@link Properties}. See {@link ConnectionResourcesBean} for the full
   * list of property names.
   *
   * @param properties The properties to configure from.
   */
  public ConnectionResourcesBean(Properties properties) {
    super();
    this.databaseType = properties.getProperty("databaseType");
    this.hostName = properties.getProperty("hostName");
    String portAsString = properties.getProperty("port");
    if (portAsString != null) this.port = Integer.parseInt(portAsString);
    this.instanceName = properties.getProperty("instanceName");
    this.databaseName = properties.getProperty("databaseName");
    this.schemaName = properties.getProperty("schemaName");
    this.userName = properties.getProperty("userName");
    String statementPoolingMaxStatementsAsString = properties.getProperty("statementPoolingMaxStatements");
    if (statementPoolingMaxStatementsAsString != null) this.statementPoolingMaxStatements = Integer.parseInt(statementPoolingMaxStatementsAsString);
    this.password = properties.getProperty("password");
  }


  /**
   * Construct from a set of properties in the format expected by {@link Properties}.
   * See {@link ConnectionResourcesBean} for the full list of property names.
   *
   * <p>The stream is assumed to use the ISO 8859-1 character encoding (see
   * {@link Properties#load(InputStream)} for more information).</p>
   *
   * @param inputStream The input stream.
   */
  public ConnectionResourcesBean(InputStream inputStream) {
    this(propertiesFromStream(inputStream));
  }


  /**
   * Construct from a set of properties in the format expected by {@link Properties}.
   * See {@link ConnectionResourcesBean} for the full list of property names.
   *
   * <p>The file is assumed to use the ISO 8859-1 character encoding (see
   * {@link Properties#load(InputStream)} for more information).</p>
   *
   * @param propertiesUrl URL to the properties file.
   */
  public ConnectionResourcesBean(URL propertiesUrl) {
    this(propertiesFromUrl(propertiesUrl));
  }


  /**
   * Construct from a set of properties in the format expected by {@link Properties}.
   * See {@link ConnectionResourcesBean} for the full list of property names.
   *
   * @param propertiesFile The properties file.
   */
  public ConnectionResourcesBean(File propertiesFile) {
    this(propertiesFromFile(propertiesFile));
  }


  private static Properties propertiesFromStream(InputStream inputStream) {
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load properties from stream", e);
    }
    return properties;
  }


  private static Properties propertiesFromUrl(URL propertiesUrl) {
    try (InputStream stream = new ByteArrayInputStream(Resources.toByteArray(propertiesUrl))) {
      return propertiesFromStream(stream);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load properties from URL [" + propertiesUrl + "]", e);
    }
  }


  private static Properties propertiesFromFile(File propertiesFile) {
    try (InputStream stream = new FileInputStream(propertiesFile)) {
      return propertiesFromStream(stream);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load properties from file [" + propertiesFile + "]", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getDatabaseType()
   */
  @Override
  public String getDatabaseType() {
    return databaseType;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setDatabaseType(java.lang.String)
   */
  @Override
  public void setDatabaseType(String databaseType) {
    this.databaseType = databaseType;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getStatementPoolingMaxStatements()
   */
  @Override
  public int getStatementPoolingMaxStatements() {
    return statementPoolingMaxStatements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setStatementPoolingMaxStatements(int)
   */
  @Override
  public void setStatementPoolingMaxStatements(int statementPoolingMaxStatements) {
    this.statementPoolingMaxStatements = statementPoolingMaxStatements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#getHostName()
   */
  @Override
  public String getHostName() {
    return hostName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setHostName(java.lang.String)
   */
  @Override
  public void setHostName(String hostName) {
    this.hostName = hostName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#getPort()
   */
  @Override
  public int getPort() {
    return port;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setPort(int)
   */
  @Override
  public void setPort(int port) {
    this.port = port;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#getInstanceName()
   */
  @Override
  public String getInstanceName() {
    return instanceName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setInstanceName(java.lang.String)
   */
  @Override
  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getDatabaseName()
   */
  @Override
  public String getDatabaseName() {
    return databaseName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setDatabaseName(java.lang.String)
   */
  @Override
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getSchemaName()
   */
  @Override
  public String getSchemaName() {
    return schemaName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setSchemaName(java.lang.String)
   */
  @Override
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#getUserName()
   */
  @Override
  public String getUserName() {
    return userName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setUserName(java.lang.String)
   */
  @Override
  public void setUserName(String userName) {
    this.userName = userName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#getPassword()
   */
  @Override
  public String getPassword() {
    return password;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setPassword(java.lang.String)
   */
  @Override
  public void setPassword(String password) {
    this.password = password;
  }


  /**
   * {@inheritDoc}
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.getClass().getSimpleName()).append(": ");
    builder.append("JDBC URL [").append(getJdbcUrl()).append("] ");
    builder.append("Username [").append(userName).append("] ");
    builder.append("Password [").append(StringUtils.isBlank(password) ? "NOT SET" : "********").append("] ");
    builder.append("Host name [").append(hostName).append("] ");
    builder.append("Port [").append(port).append("] ");
    builder.append("Database name [").append(databaseName).append("] ");
    builder.append("Schema name [").append(schemaName).append("] ");
    builder.append("Instance name [").append(instanceName).append("]");
    return builder.toString();
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (databaseName == null ? 0 : databaseName.hashCode());
    result = prime * result + (databaseType == null ? 0 : databaseType.hashCode());
    result = prime * result + (hostName == null ? 0 : hostName.hashCode());
    result = prime * result + (instanceName == null ? 0 : instanceName.hashCode());
    result = prime * result + port;
    result = prime * result + (schemaName == null ? 0 : schemaName.hashCode());
    result = prime * result + statementPoolingMaxStatements;
    result = prime * result + (userName == null ? 0 : userName.hashCode());
    return result;
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public final boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ConnectionResourcesBean other = (ConnectionResourcesBean) obj;
    if (databaseName == null) {
      if (other.databaseName != null) return false;
    } else if (!databaseName.equals(other.databaseName)) return false;
    if (!Objects.equals(databaseType, other.databaseType)) return false;
    if (hostName == null) {
      if (other.hostName != null) return false;
    } else if (!hostName.equals(other.hostName)) return false;
    if (instanceName == null) {
      if (other.instanceName != null) return false;
    } else if (!instanceName.equals(other.instanceName)) return false;
    if (port != other.port) return false;
    if (schemaName == null) {
      if (other.schemaName != null) return false;
    } else if (!schemaName.equals(other.schemaName)) return false;
    if (statementPoolingMaxStatements != other.statementPoolingMaxStatements) return false;
    if (userName == null) {
      if (other.userName != null) return false;
    } else if (!userName.equals(other.userName)) return false;
    return true;
  }
}