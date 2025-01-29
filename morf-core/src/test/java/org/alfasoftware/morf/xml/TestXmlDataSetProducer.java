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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.xml.SourceXML.readResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.DataSetConsumer.CloseState;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Test cases to check XML can be parsed to a data set consumer.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class TestXmlDataSetProducer {
  private static final Log log = LogFactory.getLog(TestXmlDataSetProducer.class);

  /**
   * Allocate a temporary folder for the tests.
   */
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  /**
   * Test we can read some XML in, the dump it back out to the same xml String.
   */
  @Test
  public void testFullXMLRoundTrip() {
    DataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(SourceXML.FULL_SAMPLE));

    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();

    DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);

    new DataSetConnector(producer, testConsumer).connect();

    assertEquals("output should be the same as input", SourceXML.FULL_SAMPLE, dummyXmlOutputStreamProvider.getXmlString().trim());
  }


  /**
   * Test we can read in an older version of the XML and that when it's written
   * back out things are up to date.
   */
  @Test
  public void testVersion1Xml() {
    DataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(SourceXML.FULL_SAMPLE_V1));

    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();

    DataSetConsumer testConsumer = new XmlDataSetConsumer(dummyXmlOutputStreamProvider);

    new DataSetConnector(producer, testConsumer).connect();

    assertEquals("output should be the same as input", SourceXML.FULL_SAMPLE_V1_UPGRADED, dummyXmlOutputStreamProvider.getXmlString().trim());
  }


  /**
   * Test we can read the reduced XML format in, the dump it back out to the same xml String.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testReducedXMLRoundTrip() {
    DataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(SourceXML.REDUCED_SAMPLE));

    DataSetConsumer consumer = mock(DataSetConsumer.class);

    new DataSetConnector(producer, consumer).connect();

    Mockito.verify(consumer).open();

    ArgumentCaptor<Table> tableCaptor = ArgumentCaptor.forClass(Table.class);
    ArgumentCaptor<Iterable> iterableCaptor = ArgumentCaptor.forClass(Iterable.class);

    Mockito.verify(consumer).table(tableCaptor.capture(), iterableCaptor.capture());

    SchemaHomology schemaHomology = new SchemaHomology(new SchemaHomology.ThrowingDifferenceWriter());
    assertTrue(
      schemaHomology.tablesMatch(
        tableCaptor.getValue(),
        table("Test")
        .columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("version", DataType.INTEGER).defaultValue("0"),
          column("bar", DataType.STRING),
          column("baz", DataType.STRING),
          column("bob", DataType.DECIMAL)
        )
      )
    );

    Mockito.verify(consumer).close(CloseState.COMPLETE);
  }


  /**
   * Test we can read the blob XML format in.
   */
  @Test
  public void testBlobXML() {
    DataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(SourceXML.BLOBBY_SAMPLE));

    assertEquals("exactly 1 table expected in schema", 1, producer.getSchema().tables().size());
    assertEquals("exactly 5 columns expected in table", 5, producer.getSchema().getTable("Test").columns().size());
    for (Record record : producer.records("Test")) {
      assertEquals("record value incorrect", "noel", record.getString("noel"));
      assertEquals("record value incorrect", "edmonds", record.getString("edmonds"));
      assertEquals("record value incorrect", "YmxvYmJ5", record.getString("blobby"));
    }
  }


  /**
   * Test that we can read some simple XML from within a zip file.
   * This test is a no-op when not run locally.
   *
   * @throws MalformedURLException if the test is broken.
   */
  @Test
  public void testContentFromWithinJar() throws MalformedURLException {
    URL testZip = getClass().getResource("test.zip");
    if (!testZip.getProtocol().equals("file")) {
      return;
    }

    // These tests are sub-optimal, but hopefully it won't blow up.
    new XmlDataSetProducer(new URL("jar:" + testZip.toExternalForm() + "!/test.txt"));
    new XmlDataSetProducer(new URL("jar:" + testZip.toExternalForm() + "!/foo/bar/"));
  }


  /**
   * Tests we can list table names from a directory
   * @throws MalformedURLException rarely
   */
  @Test
  public void testTableNamesAgainstDirectory() throws MalformedURLException {
    copyDataSet(folder.getRoot(), getClass().getResource("dataset.zip"));

    XmlDataSetProducer producer = new XmlDataSetProducer(folder.getRoot().toURI().toURL());
    testTableNamesAgainstProducer(producer);
  }


  /**
   * Tests we can list table names from an archive
   * @throws MalformedURLException rarely
   */
  @Test
  public void testTableNamesAgainstArchive() throws MalformedURLException {
    XmlDataSetProducer producer = new XmlDataSetProducer(getClass().getResource("dataset.zip"));
    testTableNamesAgainstProducer(producer);
  }


  /**
   * @param producer The producer to test.
   */
  private void testTableNamesAgainstProducer(XmlDataSetProducer producer) {
    producer.open();
    assertTrue("EntityOne", producer.getSchema().tableNames().contains("EntityOne"));
    assertTrue("EntityTwo", producer.getSchema().tableNames().contains("EntityTwo"));
    use(producer.records("EntityOne"));
    assertTrue("eNTITYoNE", producer.getSchema().tableExists("eNTITYoNE"));

    use(producer.records("eNTITYoNE"));
    assertFalse("Non existant table", producer.getSchema().tableNames().contains("NotExist"));

    assertEquals("Partitioned table names", Lists.newArrayList(), producer.getSchema().partitionedTableNames());
    assertEquals("Partition table names", Lists.newArrayList(), producer.getSchema().partitionTableNames());

    producer.close();
  }


  /**
   * Use the iterable, making sure it works.
   *
   * @param recordsIterable The iterable to use
   */
  private final void use(Iterable<Record> recordsIterable) {
    for (Record record : recordsIterable) {
      log.debug(record.hashCode());
    }
  }


  /**
   * Tests composite primary keys.
   */
  @Test
  public void testCompositePrimaryKey() {
    XmlDataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(SourceXML.COMPOSITE_PRIMARY_KEY));
    producer.open();
    assertEquals("exactly 1 table expected in schema", 1, producer.getSchema().tables().size());
    assertEquals("exactly 4 columns expected in table", 4, producer.getSchema().getTable("Test").columns().size());
    assertTrue("first column is primary key", producer.getSchema().getTable("Test").columns().get(0).isPrimaryKey());
    assertTrue("last column is primary key", producer.getSchema().getTable("Test").columns().get(3).isPrimaryKey());
    producer.close();
  }


  /**
   * Test the reading of xml containing comments.
   */
  @Test
  public void testWithComments() throws IOException {

    String input = SourceXML.readResource("testWithComments.xml");
    XmlDataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(input, "Foo"));
    producer.open();

    Table fooTable = producer.getSchema().getTable("Foo");

    assertTrue(
      new SchemaHomology().tablesMatch(
        fooTable,
        table("Foo")
          .columns(
            column("id", DataType.BIG_INTEGER).primaryKey())
          )
      );

    List<Record> records = ImmutableList.copyOf(producer.records("Foo"));

    assertEquals(40646L, records.get(0).getLong("id").longValue());
    assertEquals(40641L, records.get(3).getLong("id").longValue());

    producer.close();
  }

  @Test
  public void testPartialData() throws IOException {
    String input = SourceXML.readResource("testPartialData.xml");
    XmlDataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(input, "TestTable"));
    producer.open();

    List<Record> records = ImmutableList.copyOf(producer.records("TestTable"));

    assertEquals("1", records.get(0).getString("x"));
    assertEquals("2", records.get(1).getString("x"));

    assertEquals("ABC", records.get(0).getString("y"));
    assertEquals(null, records.get(1).getString("y"));

    producer.close();
  }


  @Test
  public void testFutureFormatFails() throws IOException {
    String input = SourceXML.readResource("testFutureFormatFails.xml");
    XmlDataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(input, "TestTable"));
    producer.open();

    try {
      ImmutableList.copyOf(producer.records("TestTable"));
      fail();
    } catch(IllegalStateException ise) {
      // ok
    } finally {
      producer.close();
    }
  }


  private void validateDataSetProducerWithNullsAndBackslashes(DataSetProducer dataSetProducer) {
    dataSetProducer.open();
    ImmutableList<Record> records = ImmutableList.copyOf(dataSetProducer.records("Foo"));
    assertEquals(new String(new char[] {'A', 0 /*null*/, 'C'}), records.get(0).getString("val"));
    assertEquals(new String(new char[] { 0 /*null*/ }), records.get(1).getString("val"));
    assertEquals("escape\\it", records.get(2).getString("val"));
    dataSetProducer.close();
  }


  @Test
  public void testWithNullCharacterReferencesV2() {
    validateDataSetProducerWithNullsAndBackslashes(new XmlDataSetProducer(new TestXmlInputStreamProvider(readResource("testWithNullCharacterReferencesV2.xml"), "Foo")));
  }


  @Test
  public void testWithNullCharacterReferencesV3() {
    validateDataSetProducerWithNullsAndBackslashes(new XmlDataSetProducer(new TestXmlInputStreamProvider(readResource("testWithNullCharacterReferencesV3.xml"), "Foo")));
  }


  @Test
  public void testWithUnusualCharacters() {
    XmlDataSetProducer dataSetProducer = new XmlDataSetProducer(new TestXmlInputStreamProvider(readResource("testWithUnusualCharacters.xml"), "testWithUnusualCharacters"));
    dataSetProducer.open();
    try {
      ImmutableList<Record> r = ImmutableList.copyOf(dataSetProducer.records("testWithUnusualCharacters"));

      assertEquals("\u0000", r.get(0).getString("characterValue"));
      assertEquals("&", r.get(38).getString("characterValue"));
      assertEquals(">", r.get(62).getString("characterValue"));
      assertEquals("A", r.get(65).getString("characterValue"));
      assertEquals("\n", r.get(10).getString("characterValue"));
      assertEquals("\r", r.get(13).getString("characterValue"));

      assertEquals("\ufffd", r.get(344).getString("characterValue"));
      assertEquals("\ufffe", r.get(345).getString("characterValue"));
      assertEquals("\uffff", r.get(346).getString("characterValue"));

    } finally {
      dataSetProducer.close();
    }
  }


  /**
   * Testing implementation to catch result XML.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static final class TestXmlInputStreamProvider implements XmlInputStreamProvider {

    private final String content;
    private final String tableName;

    /**
     * Creates the test instance.
     *
     * @param content Source for test data.
     */
    public TestXmlInputStreamProvider(String content, String tableName) {
      super();
      this.content = content;
      this.tableName = tableName;
    }


    /**
     * Creates the test instance.
     *
     * @param content Source for test data.
     */
    public TestXmlInputStreamProvider(String content) {
      this(content, "Test");
    }


    /**
     * @see org.alfasoftware.morf.xml.XmlStreamProvider#close()
     */
    @Override
    public void close() {
      // Nothing to do
    }

    /**
     * @see org.alfasoftware.morf.xml.XmlStreamProvider#open()
     */
    @Override
    public void open() {
      // Nothing to do
    }

    /**
     * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider#openInputStreamForTable(java.lang.String)
     */
    @Override
    public InputStream openInputStreamForTable(String tableName) {
      assertEquals("Table name", this.tableName.toUpperCase(), tableName.toUpperCase());
      return new ByteArrayInputStream(content.getBytes(Charsets.UTF_8));
    }

    /**
     * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider#availableStreamNames()
     */
    @Override
    public Collection<String> availableStreamNames() {
      return Arrays.asList(tableName);
    }

    /**
     * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider#tableExists(java.lang.String)
     */
    @Override
    public boolean tableExists(String name) {
      return name.equalsIgnoreCase(tableName);
    }
  }


  /**
   * @param destinationFolder
   */
  private void copyDataSet(File destinationFolder, URL sourceURL) {
    XmlDataSetProducer producer = new XmlDataSetProducer(sourceURL);
    XmlDataSetConsumer consumer = new XmlDataSetConsumer(destinationFolder);
    new DataSetConnector(producer, consumer).connect();
  }
}
