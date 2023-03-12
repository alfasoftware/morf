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

import com.google.common.collect.ImmutableList;
import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.DataSetConsumer.CloseState;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.directory.DirectoryDataSetConsumer;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
    Table metaData = SchemaUtils.table("Test").columns(
        SchemaUtils.column("id", DataType.BIG_INTEGER).primaryKey().autoNumbered(123),
        SchemaUtils.versionColumn(),
        SchemaUtils.column("bar", DataType.STRING, 10).nullable(),
        SchemaUtils.column("baz", DataType.STRING, 10).nullable(),
        SchemaUtils.column("bob", DataType.DECIMAL, 13, 2).nullable()
      ).indexes(
        SchemaUtils.index("fizz").unique().columns("bar", "baz")
      );

    testConsumer.open();
    List<Record> mockRecords = new ArrayList<>();
    mockRecords.add(DataSetUtils.record()
      .setInteger(SchemaUtils.idColumn().getName(), 1)
      .setInteger(SchemaUtils.versionColumn().getName(), 1)
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
    Table metaData = SchemaUtils.table("Test").columns(
        SchemaUtils.idColumn(),
        SchemaUtils.versionColumn(),
        SchemaUtils.column("noel", DataType.STRING, 10).nullable(),
        SchemaUtils.column("edmonds", DataType.STRING, 10).nullable(),
        SchemaUtils.column("blobby", DataType.BLOB).nullable()
      );

    testConsumer.open();
    List<Record> mockRecords = new ArrayList<>();
    mockRecords.add(DataSetUtils.record()
      .setInteger(SchemaUtils.idColumn().getName(), 1)
      .setInteger(SchemaUtils.versionColumn().getName(), 1)
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
    Table metaData = SchemaUtils.table("Test").columns(
        SchemaUtils.idColumn(),
        SchemaUtils.versionColumn(),
        SchemaUtils.column("normalColumn", DataType.STRING, 10).nullable(),
        SchemaUtils.column("col2", DataType.STRING, 10).primaryKey()
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

      Assert.assertTrue("cleared", dummyXmlOutputStreamProvider.cleared());
    }
    {
      DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
      DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider, DirectoryDataSetConsumer.ClearDestinationBehaviour.CLEAR);
      testConsumer.open();
      testConsumer.close(CloseState.COMPLETE);

      Assert.assertTrue("cleared", dummyXmlOutputStreamProvider.cleared());
    }
    {
      DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
      DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider, DirectoryDataSetConsumer.ClearDestinationBehaviour.OVERWRITE);
      testConsumer.open();
      testConsumer.close(CloseState.COMPLETE);

      Assert.assertFalse("not cleared", dummyXmlOutputStreamProvider.cleared());
    }
  }


  /**
   * Test serialisation of indexes, including the index ordering.
   */
  @Test
  public void testIndexOrdering() throws IOException {

    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
    DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);
    Table testTable = SchemaUtils.table("TestTable")
      .columns(
        SchemaUtils.column("x", DataType.DECIMAL, 10),
        SchemaUtils.column("y", DataType.DECIMAL, 10),
        SchemaUtils.column("z", DataType.DECIMAL, 10)
      ).indexes(
        SchemaUtils.index("CCC").columns("x"),  // these will get re-ordered on export
        SchemaUtils.index("AAA").columns("y"),
        SchemaUtils.index("DDD").columns("z"),
        SchemaUtils.index("BBB").columns("x", "y")
      );

    testConsumer.open();
    List<Record> mockRecords = new ArrayList<>();
    testConsumer.table(testTable, mockRecords);
    testConsumer.close(CloseState.COMPLETE);

    String expectedXML = SourceXML.readResource("testIndexOrdering.xml");
    String actualXML = dummyXmlOutputStreamProvider.getXmlString().trim();

    Assert.assertEquals("Serialised data set", expectedXML, actualXML);
  }


  @Test
  public void testWithNullCharacters() {
    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
    DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);
    Table testTable = SchemaUtils.table("TestTable")
      .columns(
        SchemaUtils.column("x", DataType.STRING, 10)
      );

    testConsumer.open();

    List<Record> records = ImmutableList.<Record>of(
      DataSetUtils.record().setString("x", "foo"),
      DataSetUtils.record().setString("x", new String(new char[] {'a', 0, 'c'})),
      DataSetUtils.record().setString("x", new String(new char[] {0})),
      DataSetUtils.record().setString("x", "string with a \\ in it"),
      DataSetUtils.record().setString("x", "some \r valid \n things \t in this one")
    );

    testConsumer.table(testTable, records);
    testConsumer.close(CloseState.COMPLETE);

    String actualXML = dummyXmlOutputStreamProvider.getXmlString().trim();
    String expectedXML = SourceXML.readResource("testWithNullCharacters.xml");

    Assert.assertEquals("Serialised data set", expectedXML, actualXML);

  }
}
