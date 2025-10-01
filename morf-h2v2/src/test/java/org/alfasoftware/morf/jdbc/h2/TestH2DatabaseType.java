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

package org.alfasoftware.morf.jdbc.h2;

import static org.alfasoftware.morf.jdbc.DatabaseTypeIdentifierTestUtils.mockDataSourceFor;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.sql.SQLException;
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.DatabaseTypeIdentifier;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.alfasoftware.morf.jdbc.JdbcUrlElements.Builder;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for DatabaseType.H2
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class TestH2DatabaseType {

  private DatabaseType databaseType;


  @Before
  public void setup() {
    databaseType = DatabaseType.Registry.findByIdentifier(H2.IDENTIFIER);
  }


  /**
   * Test the JDBC URL construction.
   */
  @Test
  public void testFormatJdbcUrl() {
    String suffix = ";DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=150000;LOB_TIMEOUT=2000;MV_STORE=TRUE;NON_KEYWORDS=YEAR,MONTH";

    assertEquals("jdbc:h2:tcp://foo.com:123/mem:alfa" + suffix, databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("foo.com").withPort(123).withDatabaseName("alfa").build()));
    assertEquals("jdbc:h2:tcp://foo.com/mem:alfa" + suffix, databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("foo.com").withDatabaseName("alfa").build()));

    assertEquals("jdbc:h2:mem:alfa" + suffix, databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withDatabaseName("alfa").build()));

    assertEquals("jdbc:h2:file:." + File.separator + "alfa" + suffix, databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withInstanceName(".").withDatabaseName("alfa").build()));
    assertEquals("jdbc:h2:file:bar" + File.separator + "alfa" + suffix, databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withInstanceName("bar" + File.separator).withDatabaseName("alfa").build()));

    assertEquals("jdbc:h2:mem:data;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=150000;LOB_TIMEOUT=2000;MV_STORE=TRUE;NON_KEYWORDS=YEAR,MONTH", databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("localhost").withDatabaseName("data").build()));
  }



  @Test(expected = UnsupportedOperationException.class)
  public void getXADataSourceForH2ShouldThrowException(){
    databaseType.getXADataSource(null, null, null);
  }


  /**
   * Test identification of a database platform.
   *
   * @throws SQLException as part of tested contract.
   */
  @Test
  public void testIdentifyFromMetaData() throws SQLException {
    // -- Unknown and resource management...
    //
    DataSource dataSource = mockDataSourceFor("FictiousDB", "9.9.9", 9, 9);
    assertEquals(Optional.empty(), new DatabaseTypeIdentifier(dataSource).identifyFromMetaData());
    verify(dataSource.getConnection()).close();

    // -- Support platforms...
    //
    assertEquals(databaseType, new DatabaseTypeIdentifier(mockDataSourceFor("H2", "1.3.167 (2012-05-23)", 1, 3)).identifyFromMetaData().get());
  }


  private Builder jdbcUrlElementBuilder() {
    return JdbcUrlElements.forDatabaseType(H2.IDENTIFIER);
  }
}
