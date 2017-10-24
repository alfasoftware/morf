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
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Test;

/**
 * Test that the data set connector works.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class TestDataSetConnector {

  /**
   * Test we can pass a data set through the connector.
   */
  @Test
  public void testBasicDataSetTransmission() {
    // This is a very simple test but at least ensures the test subject adheres to both contracts.

    MockDataSetConsumer testConsumer = new MockDataSetConsumer();

    MockDataSetProducer testProducer = new MockDataSetProducer();
    testProducer.addTable(table("foo").columns(
        idColumn(),
        versionColumn(),
        column("bar", DataType.STRING, 10),
        column("baz", DataType.STRING, 10)
      )
    );

    Table metaData = table("foo2").columns(
        idColumn(),
        versionColumn(),
        column("bar", DataType.STRING, 10),
        column("baz", DataType.STRING, 10)
      );
    testProducer.addTable(metaData, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar", "val1")
      .setString("baz", "val2"));

    new DataSetConnector(testProducer, testConsumer).connect();

    assertEquals("Data set connector", "[open, table foo [column id, column version, column bar, column baz], end table, table foo2 [column id, column version, column bar, column baz], [1, 1, val1, val2], end table, close]", testConsumer.toString());
  }

}
