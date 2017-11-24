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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.MockDataSetConsumer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases to check XML can be parsed to a data set consumer.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class TestXmlDataSetProducer {
  private static final Logger log = LoggerFactory.getLogger(TestXmlDataSetProducer.class);

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
  @Test
  public void testReducedXMLRoundTrip() {
    DataSetProducer producer = new XmlDataSetProducer(new TestXmlInputStreamProvider(SourceXML.REDUCED_SAMPLE));

    MockDataSetConsumer mockDataSetConsumer = new MockDataSetConsumer();

    new DataSetConnector(producer, mockDataSetConsumer).connect();

    assertEquals("output should be the same as input", "[open, table Test [column id, column version, column bar, column baz, column bob], end table, close]", mockDataSetConsumer.toString());
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
    producer.close();
  }


  /**
   * Use the iterable, making sure it works.
   *
   * @param recordsIterable The iterable to use
   */
  private final void use(Iterable<Record> recordsIterable) {
    for (Record record : recordsIterable) {
      log.debug(String.valueOf(record.hashCode()));
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
   * Testing implementation to catch result XML.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static final class TestXmlInputStreamProvider implements XmlInputStreamProvider {

    /**
     * Holds the test data.
     */
    private final String content;

    /**
     * Creates the test instance.
     *
     * @param content Source for test data.
     */
    public TestXmlInputStreamProvider(String content) {
      super();
      this.content = content;
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
      assertEquals("Table name", "Test", tableName);
      return new ByteArrayInputStream(content.getBytes());
    }

    /**
     * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider#availableStreamNames()
     */
    @Override
    public Collection<String> availableStreamNames() {
      return Arrays.asList("Test");
    }

    /**
     * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider#tableExists(java.lang.String)
     */
    @Override
    public boolean tableExists(String name) {
      return name.toUpperCase().equals("TEST");
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
