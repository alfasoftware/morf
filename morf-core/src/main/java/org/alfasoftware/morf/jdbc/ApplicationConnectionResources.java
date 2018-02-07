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
 * Configuration regarding database connections in applications in a
 * Java EE context. This includes:
 *
 * <ul>
 * <li>Connection pooling, see {@link #getPoolingType()}.</li>
 * <li>Transaction management, see {@link #getTransactionManagementType()}.</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public interface ApplicationConnectionResources {

  /**
   * Specifies the type of database connection pooling which should be
   * used.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   * @see ApplicationConnectionResources#getPoolingType()
   */
  public enum PoolingType {
    /**
     * The application will do no connection pooling. Connection pooling should
     * be configured at the container level.
     */
    CONTAINER,

    /**
     * A BoneCP connection pool should be used.
     */
    BONECP,

    /**
     * A HikariCP connection pool should be used.
     */
    HIKARICP,

    /**
     * An Atomikos connection pool should be used. Necessary when using Atomikos XA/JTA.
     */
    ATOMIKOS,

    /**
     * A NuoDB connection pool should be used.
     */
    NUODB
  }


  /**
   * Specifies the type of transaction management which should be used.
   *
   * @author Copyright (c) Alfa Financial Software 2011
   * @see ApplicationConnectionResources#getTransactionManagementType()
   */
  public enum TransactionManagementType {
    /**
     * The application will programatically create and manage transactions
     * locally. No external coordination will be used.
     */
    APPLICATION,

    /**
     * The application will delegate all transaction management to the
     * application container. This <em>must</em> be used for an application
     * to participate in distributed transactions; including EJB beans within
     * the application, and calls to EJB beans outside of the application.
     */
    JTA;
  }


  /**
   * @return The transaction management type to be used. Implementations should return {@link TransactionManagementType#APPLICATION}
   *  as a default, if nothing else is configured.
   */
  public TransactionManagementType getTransactionManagementType();


  /**
   * @return The pooling type to be used.
   */
  public PoolingType getPoolingType();


  /**
   * @return The connection pool size if a type of PoolingType.CONTAINER is not used.
   */
  public int getConnectionPoolSize();
}
