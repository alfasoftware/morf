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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.net.URL;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.xml.XmlDataSetProducer;
import org.apache.commons.lang.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 *  Test cases for {@link DataSetHomology}.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestDataSetHomology {

  /**
   * Test loading the same dataset produces no differences
   */
  @Test
  public void testIdendicalDataSets() {

    DataSetProducer producer1 = new XmlDataSetProducer(getResource("DataSetHomologyStart.zip"));
    DataSetProducer producer2 = new XmlDataSetProducer(getResource("DataSetHomologyStart.zip"));

    DataSetHomology dataSetHomology = new DataSetHomology();
    boolean result = dataSetHomology.dataSetProducersMatch(producer1, producer2);

    assertEquals("", StringUtils.join(dataSetHomology.getDifferences(), ", "));
    assertTrue(result);
  }

  /**
   * Test detection of differences
   */
  @Test
  public void testDifferingDataSets() {

    DataSetProducer producer1 = new XmlDataSetProducer(getResource("DataSetHomologyStart.zip"));
    DataSetProducer producer2 = new XmlDataSetProducer(getResource("DataSetHomologyDiff.zip"));

    DataSetHomology dataSetHomology = new DataSetHomology();
    boolean result = dataSetHomology.dataSetProducersMatch(producer1, producer2);

    assertEquals("Table [TableOne]: Mismatch on key id=[5][5] column [ColumnOne] row [4]: [A000000007]<>[xxx], " +
    		         "Table [TableOne]: Mismatch on key id=[8][8] column [ColumnOne] row [7]: [A000000009]<>[yyy]",
      StringUtils.join(dataSetHomology.getDifferences(), ", "));
    assertFalse(result);
  }


  /**
   * Tests that the comparison is not case sensitive with table names
   */
  @Test
  public void testDifferentCaseTableNames() {
    MockDataSetProducer testProducer1 = new MockDataSetProducer();
    Table metadata1 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("bar", DataType.STRING, 10),
        column("baz", DataType.STRING, 10)
      );
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar", "val1")
      .setString("baz", "val2"));

    MockDataSetProducer testProducer2 = new MockDataSetProducer();
    Table metadata2 = table("FOO").columns(
        idColumn(),
        versionColumn(),
        column("bar", DataType.STRING, 10),
        column("baz", DataType.STRING, 10)
      );
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar", "val1")
      .setString("baz", "val2"));

    DataSetHomology dataSetHomology = new DataSetHomology();
    boolean result = dataSetHomology.dataSetProducersMatch(testProducer1, testProducer2);

    assertEquals("Should be no differences between the table data", "", StringUtils.join(dataSetHomology.getDifferences(), ", "));
    assertTrue("Should be no differences between the tables", result);
  }


  /**
   * Tests that the comparison is not case sensitive with table names
   */
  @Test
  public void testDifferentCaseFieldNames() {
    MockDataSetProducer testProducer1 = new MockDataSetProducer();
    Table metadata1 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("bar", DataType.STRING, 10),
        column("baz", DataType.STRING, 10)
      );
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar", "val1")
      .setString("baz", "val2"));

    MockDataSetProducer testProducer2 = new MockDataSetProducer();
    Table metadata2 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("BAR", DataType.STRING, 10),
        column("bAz", DataType.STRING, 10)
      );
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar", "val1")
      .setString("baz", "val2"));

    DataSetHomology dataSetHomology = new DataSetHomology();
    boolean result = dataSetHomology.dataSetProducersMatch(testProducer1, testProducer2);

    assertEquals("Should be no differences between the field data", "", StringUtils.join(dataSetHomology.getDifferences(), ", "));
    assertTrue("Should be no differences between the fields", result);
  }


  /**
   * Test comparing decimal values which are both null
   */
  @Test
  public void testWithBothNullDecimals() {
    MockDataSetProducer testProducer1 = new MockDataSetProducer();
    Table metadata1 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "")
      .setBigDecimal("Decimal", null));

    MockDataSetProducer testProducer2 = new MockDataSetProducer();
    Table metadata2 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "")
      .setBigDecimal("Decimal", null));

    DataSetHomology dataSetHomology = new DataSetHomology();
    boolean result = dataSetHomology.dataSetProducersMatch(testProducer1, testProducer2);

    assertEquals("Should be no differences between the field data", "", StringUtils.join(dataSetHomology.getDifferences(), ", "));
    assertTrue("Should be no differences between the fields", result);
  }


  /**
   * Test comparing decimal values where one value is null
   */
  @Test
  public void testWithOneNullDecimals() {
    MockDataSetProducer testProducer1 = new MockDataSetProducer();
    Table metadata1 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "")
      .setString("Decimal", "2"));

    MockDataSetProducer testProducer2 = new MockDataSetProducer();
    Table metadata2 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "")
      .setBigDecimal("Decimal", null));

    DataSetHomology dataSetHomology = new DataSetHomology();
    boolean result = dataSetHomology.dataSetProducersMatch(testProducer1, testProducer2);

    assertEquals(
      "Expect differences between the field data",
      "Table [foo]: Mismatch on key id=[1][1] column [Decimal] row [0]: [2]<>[null]",
      StringUtils.join(dataSetHomology.getDifferences(), ", ")
    );
    assertFalse("Expect differences between the fields", result);
  }


  /**
   * Test comparing data sets which only differ on their ids and version.  By default this is fine.
   */
  @Test
  public void testIdenticalWithDifferingIdsAndVersion() {
    MockDataSetProducer testProducer1 = new MockDataSetProducer();
    Table metadata1 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "FOO")
      .setInteger("Decimal", 2));
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 2)
      .setInteger(versionColumn().getName(), 2)
      .setString("string", "BAR")
      .setInteger("Decimal", 3));

    MockDataSetProducer testProducer2 = new MockDataSetProducer();
    Table metadata2 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 2)
      .setInteger(versionColumn().getName(), 2)
      .setString("string", "FOO")
      .setInteger("Decimal", 2));
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 4)
      .setInteger(versionColumn().getName(), 4)
      .setString("string", "BAR")
      .setInteger("Decimal", 3));

    DataSetHomology dataSetHomology = new DataSetHomology();
    boolean result = dataSetHomology.dataSetProducersMatch(testProducer1, testProducer2);
    assertTrue(dataSetHomology.getDifferences().toString(), result);
  }


  /**
   * Test comparing identical data sets, ensuring we check id and version
   */
  @Test
  public void testIdenticalIncludingIdAndVersion() {
    MockDataSetProducer testProducer1 = new MockDataSetProducer();
    Table metadata1 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "FOO")
      .setInteger("Decimal", 2));
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 2)
      .setInteger(versionColumn().getName(), 2)
      .setString("string", "BAR")
      .setInteger("Decimal", 3));

    MockDataSetProducer testProducer2 = new MockDataSetProducer();
    Table metadata2 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "FOO")
      .setInteger("Decimal", 2));
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 2)
      .setInteger(versionColumn().getName(), 2)
      .setString("string", "BAR")
      .setInteger("Decimal", 3));

    assertTrue(new DataSetHomology(ImmutableMap.<String, Comparator<Record>>of(), Optional.<Collection<String>>of(ImmutableSet.<String>of())).dataSetProducersMatch(testProducer1, testProducer2));
  }


  /**
   * Ensure we can detect mismatching ids if we choose to do so.
   */
  @Test
  public void testMismatchingIdsWhenCheckingId() {
    MockDataSetProducer testProducer1 = new MockDataSetProducer();
    Table metadata1 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "FOO")
      .setInteger("Decimal", 2));
    testProducer1.addTable(metadata1, record()
      .setInteger(idColumn().getName(), 3)
      .setInteger(versionColumn().getName(), 2)
      .setString("string", "BAR")
      .setInteger("Decimal", 3));

    MockDataSetProducer testProducer2 = new MockDataSetProducer();
    Table metadata2 = table("foo").columns(
        idColumn(),
        versionColumn(),
        column("string", DataType.STRING, 10),
        column("Decimal", DataType.DECIMAL, 10)
      );
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("string", "FOO")
      .setInteger("Decimal", 2));
    testProducer2.addTable(metadata2, record()
      .setInteger(idColumn().getName(), 2)
      .setInteger(versionColumn().getName(), 2)
      .setString("string", "BAR")
      .setInteger("Decimal", 3));

    assertFalse(new DataSetHomology(ImmutableMap.<String, Comparator<Record>>of(), Optional.<Collection<String>>of(ImmutableSet.<String>of())).dataSetProducersMatch(testProducer1, testProducer2));
  }


  /**
   * Test comparing decimal values where the input data scale is not clear
   */
  @Test
  public void testWithDecimalsAndFixedScale() {

    Table table = table("Test").columns(
      column("Decimal", DataType.DECIMAL, 10, 2)
    );

    MockDataSetProducer testProducer1 = new MockDataSetProducer()
      .addTable(table, record().setBigDecimal("Decimal", new BigDecimal("10.00")));

    MockDataSetProducer testProducer2 = new MockDataSetProducer()
      .addTable(table, record().setBigDecimal("Decimal", new BigDecimal("10")));

    DataSetHomology dataSetHomology = new DataSetHomology();
    boolean result = dataSetHomology.dataSetProducersMatch(testProducer1, testProducer2);

    assertEquals("Expect no differences between the field data", "", StringUtils.join(dataSetHomology.getDifferences(), ", "));
    assertTrue("Expect no differences", result);
  }


  /**
   * Test that ordering can be applied so that comparisons of differently ordered but otherwise identical rows pass.
   */
  @Test
  public void testOrdering() {

    Table table = table("foo").columns(
      column("foo", DataType.STRING, 10),
      column("bar", DataType.DECIMAL, 10, 2)
    );

    MockDataSetProducer testProducer1 = new MockDataSetProducer()
      .addTable(table,
        record().setString("foo", "aaa").setInteger("bar", 1),
        record().setString("foo", "bbb").setInteger("bar", 1),
        record().setString("foo", "aaa").setInteger("bar", 5),
        record().setString("foo", "bbb").setInteger("bar", 5),
        record().setString("foo", "aaa").setInteger("bar", 10),
        record().setString("foo", "bbb").setInteger("bar", 20)
      );

    MockDataSetProducer testProducer2 = new MockDataSetProducer()
      .addTable(table,
        record().setString("foo", "aaa").setInteger("bar", 5),
        record().setString("foo", "aaa").setInteger("bar", 1),
        record().setString("foo", "bbb").setInteger("bar", 20),
        record().setString("foo", "aaa").setInteger("bar", 10),
        record().setString("foo", "bbb").setInteger("bar", 1),
        record().setString("foo", "bbb").setInteger("bar", 5)
      );

    RecordComparator recordComparator = new RecordComparator(table, "foo", "bar");

    Map<String, Comparator<Record>> map = new HashMap<>();
    map.put("foo", recordComparator);

    DataSetHomology dataSetHomology = new DataSetHomology(map);
    boolean result = dataSetHomology.dataSetProducersMatch(testProducer1, testProducer2);

    assertEquals("Should be no differences between the field data", "", StringUtils.join(dataSetHomology.getDifferences(), ", "));
    assertTrue("Should be no differences between the fields", result);
  }


  /**
   * Test that ordering can be applied so that comparisons of differently ordered but otherwise identical rows pass.
   */
  @Test
  public void testOrderingWithMismatchesListsMissingRecords() {

    Table table = table("foo").columns(
      column("foo", DataType.STRING, 10).primaryKey(),
      column("bar", DataType.DECIMAL, 10, 2)
    );

    MockDataSetProducer testProducer1 = new MockDataSetProducer()
      .addTable(table,
        record().setString("foo", "aaa").setInteger("bar", 1),
        record().setString("foo", "ccc").setInteger("bar", 3),
        record().setString("foo", "ddd").setInteger("bar", 4),
        record().setString("foo", "eee").setInteger("bar", 5),
        record().setString("foo", "fff").setInteger("bar", 6)
      );

    MockDataSetProducer testProducer2 = new MockDataSetProducer()
      .addTable(table,
        record().setString("foo", "eee").setInteger("bar", 5),
        record().setString("foo", "aaa").setInteger("bar", 1),
        record().setString("foo", "bbb").setInteger("bar", 2),
        record().setString("foo", "ddd").setInteger("bar", 4)
      );

    Map<String, Comparator<Record>> map = new HashMap<>();
    map.put("foo", new RecordComparator(table, "foo"));

    DataSetHomology dataSetHomology = new DataSetHomology(map);
    boolean result = dataSetHomology.dataSetProducersMatch(testProducer1, testProducer2);

    assertFalse("Should be differences", result);
    assertThat(dataSetHomology.getDifferences(), Matchers.containsInAnyOrder(
      "Table [foo]: Dataset1 is missing foo[bbb] (Dataset2=bbb,2)",
      "Table [foo]: Dataset2 is missing foo[ccc] (Dataset1=ccc,3)",
      "Table [foo]: Dataset2 is missing foo[fff] (Dataset1=fff,6)"
    ));
  }


  /**
   * Load a resource and check it exists
   * @param name The name of the resource to load
   * @return The resource {@link URL}
   */
  private URL getResource(String name) {
    URL result = getClass().getResource(name);
    if (result == null) {
      throw new IllegalArgumentException("No such resource: ["+name+"] for class "+getClass().getName());
    }
    return result;
  }

}
