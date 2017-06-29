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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.sql.SqlUtils;

/**
 * Tests for {@link AddTableFrom}.
 */
public class TestAddTableFrom {

  @Test
  public void testNoIndexesPermitted() {

    // This one is OK - no indexes
    new AddTableFrom(
      table("foo")
        .columns(column("bar", DataType.STRING, 10)),
      SqlUtils.select(SqlUtils.literal(77)));

    try {
      // Not OK - has indexes
      new AddTableFrom(
        table("foo")
          .columns(column("bar", DataType.STRING, 10))
          .indexes(SchemaUtils.index("foo_1").columns("bar")),
        SqlUtils.select(SqlUtils.literal(77)));

      fail("indexes not permitted");
    } catch(Exception e) {
      // OK
      assertTrue(e.getMessage().contains("foo"));
    }
  }
}

