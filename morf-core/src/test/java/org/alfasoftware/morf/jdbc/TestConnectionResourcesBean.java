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
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.Test;

/**
 * Tests for {@link ConnectionResourcesBean}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestConnectionResourcesBean {

  /**
   * Test initialisation from a fully populated {@link JdbcUrlElements}.
   */
  @Test
  public void testInitialiseFromJdbcUrlElementsFull() {
    // Given
    JdbcUrlElements elements = JdbcUrlElements
        .forDatabaseType("Foo")
        .withHost("Host")
        .withPort(123)
        .withDatabaseName("DB")
        .withInstanceName("instance")
        .withSchemaName("schema")
        .build();

    // When
    ConnectionResourcesBean onTest = new ConnectionResourcesBean(elements);

    // Then
    assertEquals("Foo", onTest.getDatabaseType());
    assertEquals("Host", onTest.getHostName());
    assertEquals(123, onTest.getPort());
    assertEquals("DB", onTest.getDatabaseName());
    assertEquals("instance", onTest.getInstanceName());
    assertEquals("schema", onTest.getSchemaName());
    assertNull(onTest.getUserName());
    assertNull(onTest.getPassword());
    assertEquals(0, onTest.getStatementPoolingMaxStatements());
  }


  /**
   * Test initialisation from a minimally populated {@link JdbcUrlElements}.
   * Just to make sure we're not assuming values are present.
   */
  @Test
  public void testInitialiseFromJdbcUrlElementsMinimal() {
    // Given
    JdbcUrlElements elements = JdbcUrlElements.forDatabaseType("Foo").build();

    // When
    ConnectionResourcesBean onTest = new ConnectionResourcesBean(elements);

    // Then
    assertEquals("Foo", onTest.getDatabaseType());
    assertNull(onTest.getHostName());
    assertEquals(0, onTest.getPort());
    assertNull(onTest.getDatabaseName());
    assertNull(onTest.getInstanceName());
    assertNull(onTest.getSchemaName());
    assertNull(onTest.getUserName());
    assertNull(onTest.getPassword());
    assertEquals(0, onTest.getStatementPoolingMaxStatements());
  }


  /**
   * Test initialisation from a fully-populated {@link Properties}.
   */
  @Test
  public void testInitialiseFromPropertiesFull() {
    // Given
    Properties properties = new Properties();
    properties.setProperty("databaseType", "type1");
    properties.setProperty("hostName", "host1");
    properties.setProperty("port", "678");
    properties.setProperty("databaseName", "db1");
    properties.setProperty("instanceName", "instance1");
    properties.setProperty("schemaName", "schema1");
    properties.setProperty("userName", "username1");
    properties.setProperty("password", "password1");
    properties.setProperty("statementPoolingMaxStatements", "456");
    properties.setProperty("randomStuffWeDontCareAbout", "wibble");

    // When
    ConnectionResourcesBean onTest = new ConnectionResourcesBean(properties);

    // Then
    assertEquals("type1", onTest.getDatabaseType());
    assertEquals("host1", onTest.getHostName());
    assertEquals(678, onTest.getPort());
    assertEquals("db1", onTest.getDatabaseName());
    assertEquals("instance1", onTest.getInstanceName());
    assertEquals("schema1", onTest.getSchemaName());
    assertEquals("username1", onTest.getUserName());
    assertEquals("password1", onTest.getPassword());
    assertEquals(456, onTest.getStatementPoolingMaxStatements());
  }


  /**
   * Test initialisation from a fully-populated property stream
   */
  @Test
  public void testInitialiseFromPropertyStreamFull() throws IOException {
    // Given
    Properties properties = new Properties();
    properties.setProperty("databaseType", "type1");
    properties.setProperty("hostName", "host1");
    properties.setProperty("port", "678");
    properties.setProperty("databaseName", "db1");
    properties.setProperty("instanceName", "instance1");
    properties.setProperty("schemaName", "schema1");
    properties.setProperty("userName", "username1");
    properties.setProperty("password", "password1");
    properties.setProperty("statementPoolingMaxStatements", "456");
    properties.setProperty("randomStuffWeDontCareAbout", "wibble");

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    properties.store(outputStream, "Woo, a comment");
    InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());

    // When
    ConnectionResourcesBean onTest = new ConnectionResourcesBean(inputStream);

    // Then
    assertEquals("type1", onTest.getDatabaseType());
    assertEquals("host1", onTest.getHostName());
    assertEquals(678, onTest.getPort());
    assertEquals("db1", onTest.getDatabaseName());
    assertEquals("instance1", onTest.getInstanceName());
    assertEquals("schema1", onTest.getSchemaName());
    assertEquals("username1", onTest.getUserName());
    assertEquals("password1", onTest.getPassword());
    assertEquals(456, onTest.getStatementPoolingMaxStatements());
  }


  /**
   * Test initialisation from a partially-populated {@link Properties}.
   */
  @Test
  public void testInitialiseFromPropertiesPartial() {
    // Given
    Properties properties = new Properties();

    // When
    ConnectionResourcesBean onTest = new ConnectionResourcesBean(properties);

    // Then
    assertNull(onTest.getDatabaseType());
    assertNull(onTest.getHostName());
    assertEquals(0, onTest.getPort());
    assertNull(onTest.getDatabaseName());
    assertNull(onTest.getInstanceName());
    assertNull(onTest.getSchemaName());
    assertNull(onTest.getUserName());
    assertNull(onTest.getPassword());
    assertEquals(0, onTest.getStatementPoolingMaxStatements());
  }
}