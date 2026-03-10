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

import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.AddColumnToTableUpgradeStep;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.AddDslDataChangeStep;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests that the human readable output of the upgrade path
 * finder is correct.
 *
 * @author Copyright (c) Alfa Financial Software 2026
 */
public class TestEntityHumanReadableStatements {
  private StringWriter stringWriter;


  /**
   * {@inheritDoc}
   * @see junit.framework.TestCase#setUp()
   */
  @Before
  public void setUp() throws Exception {
    stringWriter = new StringWriter();
  }

  @Ignore
  private List<String> produceLogs(List<Class<? extends UpgradeStep>> items, String startVersion) {
    ListBackedHumanReadableStatementConsumer consumer = new ListBackedHumanReadableStatementConsumer();
    PrintWriter outputStream = new PrintWriter(stringWriter);
    ListBackedEntityHumanReadableStatementConsumer consumer2 = new ListBackedEntityHumanReadableStatementConsumer(startVersion, outputStream);
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(items, true,"FOO");
    producer. produceFor(consumer, consumer2);

    return consumer2.getList();
  }

  @Ignore
  private List<String> produceLogs(List<Class<? extends UpgradeStep>> items) {
    return produceLogs(items, "v1.0.0");
  }

  @Test
  public void testSimpleEntity(){
    List<Class<? extends UpgradeStep>> items = new ArrayList<>();
    items.add(AddColumnToTableUpgradeStep.class);

    List<String> results = produceLogs(items);

    assertEquals("Should have the right number of items: " + results, 20, results.size());
    int i=0;
    assertEquals("Item should be correct", "ENTITYSTART:[table_four]", results.get(i++));
    assertEquals("Item should be correct", "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]", results.get(i++));
    assertEquals("Item should be correct", "CHANGE:[Add a non-null column to table_four called column_four [DECIMAL(9,5)], set to 10.0]", results.get(i++));
    assertEquals("Item should be correct", "STEPEND:[]", results.get(i++));
    assertEquals("Item should be correct", "ENTITYEND:[]", results.get(i++));
    assertEquals("Item should be correct", "ENTITYSTART:[table_one]", results.get(i++));
    assertEquals("Item should be correct", "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]", results.get(i++));
    assertEquals("Item should be correct", "CHANGE:[Add a nullable column to table_one called column_one [STRING(10)], set to 'A']", results.get(i++));
    assertEquals("Item should be correct", "STEPEND:[]", results.get(i++));
    assertEquals("Item should be correct", "ENTITYEND:[]", results.get(i++));
    assertEquals("Item should be correct", "ENTITYSTART:[table_three]", results.get(i++));
    assertEquals("Item should be correct", "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]", results.get(i++));
    assertEquals("Item should be correct", "CHANGE:[Add a nullable column to table_three called column_three [DECIMAL(9,5)], set to 10.0]", results.get(i++));
    assertEquals("Item should be correct", "STEPEND:[]", results.get(i++));
    assertEquals("Item should be correct", "ENTITYEND:[]", results.get(i++));
    assertEquals("Item should be correct", "ENTITYSTART:[table_two]", results.get(i++));
    assertEquals("Item should be correct", "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]", results.get(i++));
    assertEquals("Item should be correct", "CHANGE:[Add a non-null column to table_two called column_two [STRING(10)], set to 'A']", results.get(i++));
    assertEquals("Item should be correct", "STEPEND:[]", results.get(i++));
    assertEquals("Item should be correct", "ENTITYEND:[]", results.get(i++));


  }

  /**
   * Tests that the data change steps are included when requested. Both the structural change
   * step and the data change should be reported.
   */
  @Test
  public void testDataChangeEntity() {
    List<Class<? extends UpgradeStep>> items = ImmutableList.<Class<? extends UpgradeStep>>of(
        AddColumnToTableUpgradeStep.class,
        AddDslDataChangeStep.class
    );
    List<String> actual = produceLogs(items);

    List<String> expected = ImmutableList.of(
        "ENTITYSTART:[myTable]",
        "STEPSTART:[DAVEDEV-123]-[AddDslDataChangeStep]-[DSL data change]",
        String.format("CHANGE:[Add record into myTable:%n    - Set wibble to 'column1']"),
        "STEPEND:[]",
        "ENTITYEND:[]",
        "ENTITYSTART:[table_four]",
        "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]",
        "CHANGE:[Add a non-null column to table_four called column_four [DECIMAL(9,5)], set to 10.0]",
        "STEPEND:[]",
        "ENTITYEND:[]",
        "ENTITYSTART:[table_one]",
        "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]",
        "CHANGE:[Add a nullable column to table_one called column_one [STRING(10)], set to 'A']",
        "STEPEND:[]",
        "ENTITYEND:[]",
        "ENTITYSTART:[table_three]",
        "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]",
        "CHANGE:[Add a nullable column to table_three called column_three [DECIMAL(9,5)], set to 10.0]",
        "STEPEND:[]",
        "ENTITYEND:[]",
        "ENTITYSTART:[table_two]",
        "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]",
        "CHANGE:[Add a non-null column to table_two called column_two [STRING(10)], set to 'A']",
        "STEPEND:[]",
        "ENTITYEND:[]"
    );

    assertEquals(expected, actual);
  }

  @Test
  public void testEntityStartVersion_HigherThanStartVersion(){
    List<Class<? extends UpgradeStep>> items = new ArrayList<>();
    items.add(AddColumnToTableUpgradeStep.class);
    List<String> results = produceLogs(items, "v1.1.0");
    assertEquals("Should have the right number of items: " + results, 20, results.size());
  }

  @Test
  public void testEntityStartVersion_LowerThanStartVersion(){
    List<Class<? extends UpgradeStep>> items = new ArrayList<>();
    items.add(AddColumnToTableUpgradeStep.class);
    List<String> results = produceLogs(items, "v0.1.0");
    assertEquals("Should have the right number of items: " + results, 0, results.size());
  }
}
