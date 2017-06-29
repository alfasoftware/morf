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

package org.alfasoftware.morf.jdbc.nuodb;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.AbstractConnectionResources;
import org.alfasoftware.morf.jdbc.ApplicationConnectionResources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.ImmutableSortedMap;

/**
 * NuoDBDataSourceBuilder.
 * TODO Once Database supports Java 8, the references to com.nuodb.jdbc can be returned.
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class NuoDBDataSourceBuilder {
  private static final Log log = LogFactory.getLog(NuoDBDataSourceBuilder.class);

  public static DataSource build(ApplicationConnectionResources applicationConnectionResources, AbstractConnectionResources connectionDetails) {
    Properties p = new Properties();

    String jdbcUrl = connectionDetails.getJdbcUrl();

//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_URL, jdbcUrl);
    p.setProperty("url", jdbcUrl);
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_MAXACTIVE, Integer.toString(applicationConnectionResources.getConnectionPoolSize()));
    p.setProperty("maxActive", Integer.toString(applicationConnectionResources.getConnectionPoolSize()));

//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_MAXSTATEMENTS, Integer.toString(connectionDetails.getStatementPoolingMaxStatements()));
    p.setProperty("maxStatements", Integer.toString(connectionDetails.getStatementPoolingMaxStatements()));
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_MAXSTATEMENTSPERCONNECTION, Integer.toString(connectionDetails.getStatementPoolingMaxStatements()));
    p.setProperty("maxStatementsPerConnection", Integer.toString(connectionDetails.getStatementPoolingMaxStatements()));
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_INITIALSIZE, "1");
    p.setProperty("initialSize", "1");
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_MINIDLE, "1");
    p.setProperty("minIdle", "1");
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_MAXAGE, "300000"); // Automatically rebalance connections every 5 minutes.
    p.setProperty("maxAge", "300000"); // Automatically rebalance connections every 5 minutes.
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_TESTONRETURN, "true"); // apparently recommended
    p.setProperty("testOnReturn", "true"); // apparently recommended
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_VALIDATIONQUERY, "SELECT 1 from dual;");
    p.setProperty("validationQuery", "SELECT 1 from dual;");
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_DEFAULTAUTOCOMMIT, "false");
    p.setProperty("defaultAutoCommit", "false");
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_USER, connectionDetails.getUserName());
    p.setProperty("user", connectionDetails.getUserName());
//    p.setProperty(com.nuodb.jdbc.DataSource.PROP_PASSWORD, connectionDetails.getPassword());
    p.setProperty("password", connectionDetails.getPassword());

    log.info("Building NuoDB DataSource with properties ["+ImmutableSortedMap.copyOf(p)+"]");


    try {
      Class<?> clazz = Class.forName("com.nuodb.jdbc.DataSource");
      Constructor<?> constructor = clazz.getConstructor(Properties.class);
      return (DataSource) constructor.newInstance(p);
    }
    catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

}

