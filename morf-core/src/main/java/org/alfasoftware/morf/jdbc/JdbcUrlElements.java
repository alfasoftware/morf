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


/**
 * Bean representation of the decoded elements of a JDBC URL.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public final class JdbcUrlElements {

  private final String databaseType;
  private final String hostName;
  private final int port;
  private final String instanceName;
  private final String databaseName;
  private final String schemaName;


  /**
   * Builder pattern constructor.
   *
   * @param databaseType The database type identifier (see {@link DatabaseType#identifier()}).
   * @return The builder.
   */
  public static Builder forDatabaseType(String databaseType) {
    return new Builder(databaseType);
  }


  /**
   * Constructor.
   *
   * @param databaseType The database type identifier. See {@link DatabaseType#identifier()}.
   * @param hostName Server host name.
   * @param port TCP/IP Port to connect to.
   * @param instanceName Database instance name.
   * @param databaseName Database schema within the database instance.
   * @param schemaName For vendors that support multiple schemas within a database.
   */
  private JdbcUrlElements(String databaseType, String hostName, int port, String instanceName, String databaseName, String schemaName) {
    super();
    this.databaseType = databaseType;
    this.hostName = hostName;
    this.port = port;
    this.instanceName = instanceName;
    this.databaseName = databaseName;
    this.schemaName = schemaName;
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (databaseType == null ? 0 : databaseType.hashCode());
    result = prime * result + (databaseName == null ? 0 : databaseName.hashCode());
    result = prime * result + (hostName == null ? 0 : hostName.hashCode());
    result = prime * result + (instanceName == null ? 0 : instanceName.hashCode());
    result = prime * result + port;
    result = prime * result + (schemaName == null ? 0 : schemaName.hashCode());
    return result;
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    JdbcUrlElements other = (JdbcUrlElements) obj;
    if (databaseType == null) {
      if (other.databaseType != null) return false;
    } else if (!databaseType.equals(other.databaseType)) return false;
    if (databaseName == null) {
      if (other.databaseName != null) return false;
    } else if (!databaseName.equals(other.databaseName)) return false;
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
    return true;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "JdbcUrlElements [databaseTypeIdentifier=" + databaseType + ", host=" + hostName + ", port=" + port + ", instanceName=" + instanceName + ", databaseName=" + databaseName
        + ", schemaName=" + schemaName + "]";
  }


  /**
   * @return the database type identifier
   *
   * @see DatabaseType#identifier()
   * @see DatabaseType.Registry#findByIdentifier(String)
   */
  public String getDatabaseType() {
    return databaseType;
  }


  /**
   * @return the server host name.
   */
  public String getHostName() {
    return hostName;
  }


  /**
   * @return the TCP/IP Port to connect to.
   */
  public int getPort() {
    return port;
  }


  /**
   * @return the instance name.  The meaning of this varies between database types.
   */
  public String getInstanceName() {
    return instanceName;
  }


  /**
   * @return the database name.  The meaning of this varies between database types.
   */
  public String getDatabaseName() {
    return databaseName;
  }


  /**
   * @return the schema name.  The meaning of this varies between database types.
   */
  public String getSchemaName() {
    return schemaName;
  }


  /**
   * Builder for {@link JdbcUrlElements}.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  public static final class Builder {

    private final String databaseTypeIdentifier;
    private String host;
    private int port;
    private String instanceName;
    private String databaseName;
    private String schemaName;

    private Builder(String databaseTypeIdentifier) {
      this.databaseTypeIdentifier = databaseTypeIdentifier;
    }


    /**
     * Sets the host. Defaults to null (no host specified).
     *
     * @param host The host name.
     * @return this
     */
    public Builder withHost(String host) {
      this.host = host;
      return this;
    }


    /**
     * Sets the port. Defaults to 0 (no port specified).
     *
     * @param port The port number.
     * @return this
     */
    public Builder withPort(int port) {
      this.port = port;
      return this;
    }


    /**
     * Sets the instance name. Defaults to null (no instance specified).
     *
     * @param instanceName The instance name.
     * @return this
     */
    public Builder withInstanceName(String instanceName) {
      this.instanceName = instanceName;
      return this;
    }


    /**
     * Sets the database name. Defaults to null (no database specified).
     *
     * @param databaseName The database name
     * @return this
     */
    public Builder withDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }


    /**
     * Sets the schema name. Defaults to null (no schema specified).
     *
     * @param schemaName The schema name
     * @return this
     */
    public Builder withSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }


    /**
     * @return The {@link JdbcUrlElements}.
     */
    public JdbcUrlElements build() {
      return new JdbcUrlElements(databaseTypeIdentifier, host, port, instanceName, databaseName, schemaName);
    }
  }
}