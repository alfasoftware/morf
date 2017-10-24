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

package org.alfasoftware.morf.dataset;

import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Comparator;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Test;

/**
 * Tests for {@link RecordComparator}.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestRecordComparator {

  /**
   * Test the comparison works
   */
  @Test
  public void testComparison() {

    Table table = table("x").columns(
      column("foo", DataType.STRING, 10),
      column("bar", DataType.DECIMAL, 10, 2),
      column("baz", DataType.INTEGER, 10)
    );

    // compare the first two columns
    Comparator<Record> recordComparator = new RecordComparator(table, "foo", "bar");

    // sanity check the way comparators work
    assertEquals(1, "y".compareTo("x"));

    // EQUALS

    assertEquals(0,
      recordComparator.compare(
        record().setString("foo", "x").setInteger("bar", 1).setInteger("baz", 2),
        record().setString("foo", "x").setInteger("bar", 1).setInteger("baz", 2)
      )
    );

    assertEquals(0,
      recordComparator.compare(
        record().setString("foo", "x").setInteger("bar", 1).setString("baz", "2"),
        record().setString("foo", "x").setString("bar", "1").setInteger("baz", 2)
      )
    );

    assertEquals(0,
      recordComparator.compare(
        record().setString("foo", "x").setInteger("bar", -1).setString("baz", "-2"),
        record().setString("foo", "x").setString("bar", "-1").setInteger("baz", -2)
      )
    );

    // GREATER THAN - FIELD 1

    assertEquals(1,
      recordComparator.compare(
        record().setString("foo", "y").setInteger("bar", 1).setInteger("baz", 2),
        record().setString("foo", "x").setInteger("bar", 1).setInteger("baz", 2)
      )
    );

    // LESS THAN - FIELD 1

    assertEquals(-1,
      recordComparator.compare(
        record().setString("foo", "x").setInteger("bar", 1).setInteger("baz", 2),
        record().setString("foo", "y").setInteger("bar", 1).setInteger("baz", 2)
      )
    );

    // GREATER THAN - FIELD 2

    assertEquals(1,
      recordComparator.compare(
        record().setString("foo", "x").setInteger("bar", 2).setInteger("baz", 2),
        record().setString("foo", "x").setInteger("bar", 1).setInteger("baz", 2)
      )
    );

    assertEquals(1,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "2").setInteger("baz", 2),
        record().setString("foo", "x").setInteger("bar", 1).setInteger("baz", 2)
      )
    );

    assertEquals(1,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "0").setInteger("baz", 2),
        record().setString("foo", "x").setInteger("bar", -1).setInteger("baz", 2)
      )
    );

    // LESS THAN - FIELD 2

    assertEquals(-1,
      recordComparator.compare(
        record().setString("foo", "x").setInteger("bar", 1).setInteger("baz", 2),
        record().setString("foo", "x").setInteger("bar", 2).setInteger("baz", 2)
      )
    );

    assertEquals(-1,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "1").setInteger("baz", 2),
        record().setString("foo", "x").setString("bar", "2").setInteger("baz", 2)
      )
    );

    assertEquals(-1,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "-1").setInteger("baz", 2),
        record().setString("foo", "x").setString("bar", "0").setInteger("baz", 2)
      )
    );

    // check integers are compared with numeric logic

    assertEquals(1,
      recordComparator.compare(
        record().setString("foo", "x").setInteger("bar", 10).setInteger("baz", 2),
        record().setString("foo", "x").setInteger("bar", 2).setInteger("baz", 2)
      )
    );

    assertEquals(1,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "10").setInteger("baz", 2),
        record().setString("foo", "x").setString("bar", "2").setInteger("baz", 2)
      )
    );

    assertEquals(-1,
      recordComparator.compare(
        record().setString("foo", "x").setInteger("bar", 2).setInteger("baz", 2),
        record().setString("foo", "x").setInteger("bar", 10).setInteger("baz", 2)
      )
    );

    assertEquals(-1,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "2").setInteger("baz", 2),
        record().setString("foo", "x").setString("bar", "10").setInteger("baz", 2)
      )
    );

    // check decimals are compared with numeric logic

    assertEquals(1,
      recordComparator.compare(
        record().setString("foo", "x").setBigDecimal("bar", new BigDecimal("1.2")).setInteger("baz", 2),
        record().setString("foo", "x").setBigDecimal("bar", new BigDecimal("1.1")).setInteger("baz", 2)
      )
    );

    assertEquals(1,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "1.2").setInteger("baz", 2),
        record().setString("foo", "x").setString("bar", "1.1").setInteger("baz", 2)
      )
    );

    assertEquals(-1,
      recordComparator.compare(
        record().setString("foo", "x").setBigDecimal("bar", new BigDecimal("1.1")).setInteger("baz", 2),
        record().setString("foo", "x").setBigDecimal("bar", new BigDecimal("1.2")).setInteger("baz", 2)
      )
    );

    assertEquals(-1,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "1.1").setInteger("baz", 2),
        record().setString("foo", "x").setString("bar", "1.2").setInteger("baz", 2)
      )
    );

    // Make sure we're ignoring the last column

    assertEquals(0,
      recordComparator.compare(
        record().setString("foo", "x").setString("bar", "1.1").setInteger("baz", 2),
        record().setString("foo", "x").setString("bar", "1.1").setInteger("baz", 3)
      )
    );
  }
}
