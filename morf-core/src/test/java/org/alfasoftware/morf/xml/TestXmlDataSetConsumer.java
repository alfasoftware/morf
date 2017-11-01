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

package org.alfasoftware.morf.xml;

import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.DataSetConsumer.CloseState;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.xml.XmlDataSetConsumer.ClearDestinationBehaviour;
import org.junit.Test;

/**
 * Test that we can convert data sets to xml.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class TestXmlDataSetConsumer {

  /**
   * Test basic xml serialisation.
   */
  @Test
  public void testBasicSerialisation() {

    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
    DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);
    Table metaData = table("Test").columns(
        column("id", DataType.BIG_INTEGER, 19, 0).primaryKey().autoNumbered(123),
        versionColumn(),
        column("bar", DataType.STRING, 10).nullable(),
        column("baz", DataType.STRING, 10).nullable(),
        column("bob", DataType.DECIMAL, 13, 2).nullable()
      ).indexes(        index("fizz").unique().columns("bar", "baz")
      );

    testConsumer.open();
    List<Record> mockRecords = new ArrayList<>();
    mockRecords.add(record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar", "abc")
      .setString("baz", "123")
      .setString("bob", "456.78"));
    testConsumer.table(metaData, mockRecords);
    testConsumer.close(CloseState.COMPLETE);

    assertEquals("Serialised data set", SourceXML.FULL_SAMPLE, dummyXmlOutputStreamProvider.getXmlString().trim());
  }


  /**
   * Test xml serialisation with a blob field
   */
  @Test
  public void testSerialisationWithBlob() {

    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
    DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);
    Table metaData = table("Test").columns(
        idColumn(),
        versionColumn(),
        column("noel", DataType.STRING, 10).nullable(),
        column("edmonds", DataType.STRING, 10).nullable(),
        column("blobby", DataType.BLOB, 100, 0).nullable()
      );

    testConsumer.open();
    List<Record> mockRecords = new ArrayList<>();
    mockRecords.add(record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("noel", "noel")
      .setString("edmonds", "edmonds")
      .setString("blobby", "YmxvYmJ5"));
    testConsumer.table(metaData, mockRecords);
    testConsumer.close(CloseState.COMPLETE);

    assertEquals("Serialised data set", SourceXML.BLOBBY_SAMPLE, dummyXmlOutputStreamProvider.getXmlString().trim());
  }


  /**
   * Test composite primary keys.
   */
  @Test
  public void testCompositePrimaryKeys() {

    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
    DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);
    Table metaData = table("Test").columns(
        idColumn(),
        versionColumn(),
        column("normalColumn", DataType.STRING, 10).nullable(),
        column("col2", DataType.STRING, 10).primaryKey()
      );

    testConsumer.open();
    List<Record> mockRecords = new ArrayList<>();
    testConsumer.table(metaData, mockRecords);
    testConsumer.close(CloseState.COMPLETE);

    assertEquals("Serialised data set", SourceXML.COMPOSITE_PRIMARY_KEY, dummyXmlOutputStreamProvider.getXmlString().trim());
  }


  /**
   * Test that the output clearing can be controlled.
   */
  @Test
  public void testClearDestinationBehaviour() {
    {
      DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
      DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);
      testConsumer.open();
      testConsumer.close(CloseState.COMPLETE);

      assertTrue("cleared", dummyXmlOutputStreamProvider.cleared());
    }
    {
      DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
      DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider, ClearDestinationBehaviour.CLEAR);
      testConsumer.open();
      testConsumer.close(CloseState.COMPLETE);

      assertTrue("cleared", dummyXmlOutputStreamProvider.cleared());
    }
    {
      DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
      DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider, ClearDestinationBehaviour.OVERWRITE);
      testConsumer.open();
      testConsumer.close(CloseState.COMPLETE);

      assertFalse("not cleared", dummyXmlOutputStreamProvider.cleared());
    }
  }


  /**
   * Test serialisation of indexes, including the index ordering.
   */
  @Test
  public void testIndexOrdering() throws IOException {

    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
    DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);
    Table testTable = table("TestTable")
      .columns(
        column("x", DataType.DECIMAL, 10),
        column("y", DataType.DECIMAL, 10),
        column("z", DataType.DECIMAL, 10)
      ).indexes(
        index("CCC").columns("x"),  // these will get re-ordered on export
        index("AAA").columns("y"),
        index("DDD").columns("z"),
        index("BBB").columns("x", "y")
      );

    testConsumer.open();
    List<Record> mockRecords = new ArrayList<>();
    testConsumer.table(testTable, mockRecords);
    testConsumer.close(CloseState.COMPLETE);

    String expectedXML = SourceXML.readResource("testIndexOrdering.xml");
    String actualXML = dummyXmlOutputStreamProvider.getXmlString().trim();

    assertEquals("Serialised data set", expectedXML, actualXML);
  }
  

  @Test
  public void testWithNullCharacters() {
    fail();
  }
}
