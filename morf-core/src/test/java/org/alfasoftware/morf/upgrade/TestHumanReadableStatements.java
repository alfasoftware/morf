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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.AddColumnToTableUpgradeStep;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.AddDslDataChangeStep;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.ChangeColumnUpgradeStep;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.UpgradeStep5010;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.UpgradeStep5010Four;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.UpgradeStep5010Three;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.UpgradeStep5010Two;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.UpgradeStep508;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.UpgradeStep509;
import org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0.UpgradeStep511;
import com.google.common.collect.ImmutableList;

/**
 * Tests that the human readable output of the upgrade path
 * finder is correct.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestHumanReadableStatements {


  /**
   * {@inheritDoc}
   * @see junit.framework.TestCase#setUp()
   */
  @Before
  public void setUp() throws Exception {
  }


  /**
   * Tests that a simple one step upgrade works.
   */
  @Test
  public void testSimpleUpgrade() {
    List<Class<? extends UpgradeStep>> items = new ArrayList<>();

    items.add(AddColumnToTableUpgradeStep.class);

    ListBackedHumanReadableStatementConsumer consumer = new ListBackedHumanReadableStatementConsumer();
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(items);
    producer.produceFor(consumer);

    List<String> results = consumer.getList();

    assertEquals("Should have the right number of items: " + results, 8, results.size());
    int i=0;
    assertEquals("Item should be correct", "VERSIONSTART:[ALFA v1.0.0]", results.get(i++));
    assertEquals("Item should be correct", "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]", results.get(i++));
    assertEquals("Item should be correct", "CHANGE:[Add a nullable column to table_one called column_one [STRING(10)], set to 'A']", results.get(i++));
    assertEquals("Item should be correct", "CHANGE:[Add a non-null column to table_two called column_two [STRING(10)], set to 'A']", results.get(i++));
    assertEquals("Item should be correct", "CHANGE:[Add a nullable column to table_three called column_three [DECIMAL(9,5)], set to 10.0]", results.get(i++));
    assertEquals("Item should be correct", "CHANGE:[Add a non-null column to table_four called column_four [DECIMAL(9,5)], set to 10.0]", results.get(i++));
    assertEquals("Item should be correct", "STEPEND:[AddColumnToTableUpgradeStep]", results.get(i++));
    assertEquals("Item should be correct", "VERSIONEND:[ALFA v1.0.0]", results.get(i));
  }


  /**
   * Tests that a simple two step upgrade for different versions works.
   */
  @Test
  public void testSimpleTwoStepUpgrade() {
    List<Class<? extends UpgradeStep>> items = new ArrayList<>();

    items.add(AddColumnToTableUpgradeStep.class);
    items.add(ChangeColumnUpgradeStep.class);

    ListBackedHumanReadableStatementConsumer consumer = new ListBackedHumanReadableStatementConsumer();
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(items);
    producer.produceFor(consumer);

    List<String> results = consumer.getList();

    assertEquals("Should have the right number of items", 18, results.size());
    int i=0;
    assertEquals("Item should be correct", "VERSIONSTART:[ALFA v1.0.0]", results.get(i++));
    assertEquals("Item  2 should be correct", "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]", results.get(i++));
    assertEquals("Item  3 should be correct", "CHANGE:[Add a nullable column to table_one called column_one [STRING(10)], set to 'A']", results.get(i++));
    assertEquals("Item  4 should be correct", "CHANGE:[Add a non-null column to table_two called column_two [STRING(10)], set to 'A']", results.get(i++));
    assertEquals("Item  5 should be correct", "CHANGE:[Add a nullable column to table_three called column_three [DECIMAL(9,5)], set to 10.0]", results.get(i++));
    assertEquals("Item  6 should be correct", "CHANGE:[Add a non-null column to table_four called column_four [DECIMAL(9,5)], set to 10.0]", results.get(i++));
    assertEquals("Item  7 should be correct", "STEPEND:[AddColumnToTableUpgradeStep]", results.get(i++));
    assertEquals("Item 10 should be correct", "STEPSTART:[SAMPLE-2]-[ChangeColumnUpgradeStep]-[Change some columns on some tables]", results.get(i++));
    assertEquals("Item 11 should be correct", "CHANGE:[Change column column_one on table_one from nullable STRING(10) to non-null STRING(10)]", results.get(i++));
    assertEquals("Item 12 should be correct", "CHANGE:[Change column column_two on table_two from non-null STRING(10) to nullable STRING(10)]", results.get(i++));
    assertEquals("Item 13 should be correct", "CHANGE:[Change column column_three on table_three from nullable DECIMAL(9,5) to non-null DECIMAL(9,5)]", results.get(i++));
    assertEquals("Item 14 should be correct", "CHANGE:[Change column column_four on table_four from non-null DECIMAL(9,5) to nullable DECIMAL(9,5)]", results.get(i++));
    assertEquals("Item 15 should be correct", "CHANGE:[Change column column_five on table_five from non-null DECIMAL(9,0) to non-null DECIMAL(9,5)]", results.get(i++));
    assertEquals("Item 16 should be correct", "CHANGE:[Change column column_six on table_six from non-null DECIMAL(9,5) to non-null DECIMAL(9,0)]", results.get(i++));
    assertEquals("Item 17 should be correct", "CHANGE:[Change column column_seven on table_seven from non-null DECIMAL(9,0) to non-null STRING(9)]", results.get(i++));
    assertEquals("Item 18 should be correct", "CHANGE:[Change column column_eight on table_eight from non-null STRING(9) to non-null DECIMAL(9,0)]", results.get(i++));
    assertEquals("Item 19 should be correct", "STEPEND:[ChangeColumnUpgradeStep]", results.get(i++));
    assertEquals("Item should be correct", "VERSIONEND:[ALFA v1.0.0]", results.get(i));
  }


  /**
   * Tests that the upgrade comes out in the right order.
   */
  @Test
  public void testSimpleTwoStepSorted() {
    List<Class<? extends UpgradeStep>> items = new ArrayList<>();

    items.add(UpgradeStep509.class);
    items.add(UpgradeStep5010Two.class);
    items.add(UpgradeStep508.class);
    items.add(UpgradeStep5010.class);

    ListBackedHumanReadableStatementConsumer consumer = new ListBackedHumanReadableStatementConsumer();
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(items);
    producer.produceFor(consumer);

    List<String> results = consumer.getList();

    assertEquals("Should have the right number of items", 10, results.size());
    int i=0;
    assertEquals("Item should be correct", "VERSIONSTART:[ALFA v1.0.0]", results.get(i++));
    assertEquals("Item  2 should be correct", "STEPSTART:[SAMPLE-5]-[UpgradeStep508]-[5.0.8 Upgrade Step]", results.get(i++));
    assertEquals("Item  3 should be correct", "STEPEND:[UpgradeStep508]", results.get(i++));
    assertEquals("Item  6 should be correct", "STEPSTART:[SAMPLE-6]-[UpgradeStep509]-[5.0.9 Upgrade Step]", results.get(i++));
    assertEquals("Item  7 should be correct", "STEPEND:[UpgradeStep509]", results.get(i++));
    assertEquals("Item 10 should be correct", "STEPSTART:[SAMPLE-4]-[UpgradeStep5010]-[5.0.10 Upgrade Step]", results.get(i++));
    assertEquals("Item 11 should be correct", "STEPEND:[UpgradeStep5010]", results.get(i++));
    assertEquals("Item 12 should be correct", "STEPSTART:[SAMPLE-3]-[UpgradeStep5010Two]-[5.0.10 Upgrade Step (2)]", results.get(i++));
    assertEquals("Item 13 should be correct", "STEPEND:[UpgradeStep5010Two]", results.get(i++));
    assertEquals("Item should be correct", "VERSIONEND:[ALFA v1.0.0]", results.get(i));
  }


  /**
   * Tests that the upgrade comes out in the right order.
   * We expect that the versioning overrides the sequence annotation
   * when creating the changelog.
   */
  @Test
  public void testFourStepSortedByVersionThenSequence() {
    List<Class<? extends UpgradeStep>> items = new ArrayList<>();

    items.add(UpgradeStep509.class);
    items.add(UpgradeStep5010Two.class);
    items.add(UpgradeStep508.class);
    items.add(UpgradeStep5010.class);
    items.add(UpgradeStep5010Three.class);
    items.add(UpgradeStep5010Four.class);


    ListBackedHumanReadableStatementConsumer consumer = new ListBackedHumanReadableStatementConsumer();
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(items);
    producer.produceFor(consumer);

    List<String> results = consumer.getList();

    assertEquals("Should have the right number of items", 18, results.size());
    int i=0;
    assertEquals("Item should be correct", "VERSIONSTART:[ALFA v1.0.0]", results.get(i++));
    assertEquals("Item  2 should be correct", "STEPSTART:[SAMPLE-5]-[UpgradeStep508]-[5.0.8 Upgrade Step]", results.get(i++));
    assertEquals("Item  3 should be correct", "STEPEND:[UpgradeStep508]", results.get(i++));
    assertEquals("Item  6 should be correct", "STEPSTART:[SAMPLE-6]-[UpgradeStep509]-[5.0.9 Upgrade Step]", results.get(i++));
    assertEquals("Item  7 should be correct", "STEPEND:[UpgradeStep509]", results.get(i++));
    assertEquals("Item 10 should be correct", "STEPSTART:[SAMPLE-4]-[UpgradeStep5010]-[5.0.10 Upgrade Step]", results.get(i++));
    assertEquals("Item 11 should be correct", "STEPEND:[UpgradeStep5010]", results.get(i++));
    assertEquals("Item 12 should be correct", "STEPSTART:[SAMPLE-3]-[UpgradeStep5010Two]-[5.0.10 Upgrade Step (2)]", results.get(i++));
    assertEquals("Item 13 should be correct", "STEPEND:[UpgradeStep5010Two]", results.get(i++));
    assertEquals("Item should be correct", "VERSIONEND:[ALFA v1.0.0]", results.get(i++));
    assertEquals("Item should be correct", "VERSIONSTART:[ALFA v1.0.3]", results.get(i++));
    assertEquals("Item 14 should be correct", "STEPSTART:[SAMPLE-8]-[UpgradeStep5010Three]-[5.0.10 Upgrade Step (3)]", results.get(i++));
    assertEquals("Item 15 should be correct", "STEPEND:[UpgradeStep5010Three]", results.get(i++));
    assertEquals("Item should be correct", "VERSIONEND:[ALFA v1.0.3]", results.get(i++));
    assertEquals("Item should be correct", "VERSIONSTART:[ALFA v1.0.24]", results.get(i++));
    assertEquals("Item 15 should be correct", "STEPSTART:[SAMPLE-8]-[UpgradeStep5010Four]-[5.0.10 Upgrade Step (4)]", results.get(i++));
    assertEquals("Item 16 should be correct", "STEPEND:[UpgradeStep5010Four]", results.get(i++));
    assertEquals("Item should be correct", "VERSIONEND:[ALFA v1.0.24]", results.get(i));
  }


  /**
   * Tests that the multiple permutations of versions are compared correctly
   */
  @Test
  public void testDirectionVersionComparison() {
    assertEquals("5.3.1 less than 5.3.2",Integer.valueOf(-1), HumanReadableStatementProducer.versionCompare("5.3.1", "5.3.2"));
    assertEquals("5.3.2 greater than 5.3.1",Integer.valueOf(1), HumanReadableStatementProducer.versionCompare("5.3.2", "5.3.1"));
    assertEquals("5.3.3 equals 5.3.3",Integer.valueOf(0), HumanReadableStatementProducer.versionCompare("5.3.3", "5.3.3"));

    assertEquals("5.3.1.2.3.4 less than 5.3.1.2.3.5",Integer.valueOf(-1), HumanReadableStatementProducer.versionCompare("5.3.1.2.3.4", "5.3.1.2.3.5"));
    assertEquals("5.3.1 less than 5.3.1.2",Integer.valueOf(-1), HumanReadableStatementProducer.versionCompare("5.3.1", "5.3.1.2"));

    assertEquals("5.3.1a less than 5.3.2a",Integer.valueOf(-1), HumanReadableStatementProducer.versionCompare("5.3.1a", "5.3.2a"));
    assertEquals("5.3.2a greater than 5.3.1a",Integer.valueOf(1), HumanReadableStatementProducer.versionCompare("5.3.2a", "5.3.1a"));
    assertEquals("5.3.3a less than 5.3.3b",Integer.valueOf(-1), HumanReadableStatementProducer.versionCompare("5.3.3a", "5.3.3b"));
    assertEquals("5.3.3a equals 5.3.3a",Integer.valueOf(0), HumanReadableStatementProducer.versionCompare("5.3.3a", "5.3.3a"));
    assertEquals("5.3.3a.a less than 5.3.3a.b",Integer.valueOf(-1), HumanReadableStatementProducer.versionCompare("5.3.3a.a", "5.3.3a.b"));
  }

  /**
   * Tests that the upgrade comes out in the right order.
   */
  @Test
  public void testRemoveMultipleColumns() {
    List<Class<? extends UpgradeStep>> items = ImmutableList.<Class<? extends UpgradeStep>>of(
      UpgradeStep511.class
    );

    ListBackedHumanReadableStatementConsumer consumer = new ListBackedHumanReadableStatementConsumer();
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(items);
    producer.produceFor(consumer);

    List<String> actual = consumer.getList();

    List<String> expected = ImmutableList.of(
      "VERSIONSTART:[ALFA v1.0.0]",
      "STEPSTART:[SAMPLE-7]-[UpgradeStep511]-[5.1.1 Upgrade Step]",
      "CHANGE:[Remove column abc from SomeTable]",
      "CHANGE:[Remove column def from SomeTable]",
      "STEPEND:[UpgradeStep511]",
      "VERSIONEND:[ALFA v1.0.0]"
    );

    assertEquals(expected, actual);
  }


  /**
   * Tests that the data change steps are omitted by default. Only the structural change step
   * should be reported in detail.
   *
   * <p>Note that upgrade steps containing data changes are displayed as just the Jira ID and
   * description. For example, see the [WEB-11424] upgrade step (adding a transaction code) on
   * the sample file attached to [WEB-29077].</p>
   */
  @Test
  public void testDataChangeStepOmission() {
    List<Class<? extends UpgradeStep>> items = ImmutableList.<Class<? extends UpgradeStep>>of(
      AddColumnToTableUpgradeStep.class,
      AddDslDataChangeStep.class
    );

    ListBackedHumanReadableStatementConsumer consumer = new ListBackedHumanReadableStatementConsumer();
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(items);
    producer.produceFor(consumer);

    List<String> actual = consumer.getList();

    List<String> expected = ImmutableList.of(
      "VERSIONSTART:[ALFA v1.0.0]",
      "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]",
      "CHANGE:[Add a nullable column to table_one called column_one [STRING(10)], set to 'A']",
      "CHANGE:[Add a non-null column to table_two called column_two [STRING(10)], set to 'A']",
      "CHANGE:[Add a nullable column to table_three called column_three [DECIMAL(9,5)], set to 10.0]",
      "CHANGE:[Add a non-null column to table_four called column_four [DECIMAL(9,5)], set to 10.0]",
      "STEPEND:[AddColumnToTableUpgradeStep]",
      "STEPSTART:[DAVEDEV-123]-[AddDslDataChangeStep]-[DSL data change]",
      "STEPEND:[AddDslDataChangeStep]",
      "VERSIONEND:[ALFA v1.0.0]"
    );

    assertEquals(expected, actual);
  }


  /**
   * Tests that the data change steps are included when requested. Both the structural change
   * step and the data change should be reported.
   */
  @Test
  public void testDataChangeInclusion() {
    List<Class<? extends UpgradeStep>> items = ImmutableList.<Class<? extends UpgradeStep>>of(
      AddColumnToTableUpgradeStep.class,
      AddDslDataChangeStep.class
    );

    ListBackedHumanReadableStatementConsumer consumer = new ListBackedHumanReadableStatementConsumer();
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(items, true, "FOO");
    producer.produceFor(consumer);

    List<String> actual = consumer.getList();

    List<String> expected = ImmutableList.of(
      "VERSIONSTART:[ALFA v1.0.0]",
      "STEPSTART:[SAMPLE-1]-[AddColumnToTableUpgradeStep]-[Adds new columns to tables]",
      "CHANGE:[Add a nullable column to table_one called column_one [STRING(10)], set to 'A']",
      "CHANGE:[Add a non-null column to table_two called column_two [STRING(10)], set to 'A']",
      "CHANGE:[Add a nullable column to table_three called column_three [DECIMAL(9,5)], set to 10.0]",
      "CHANGE:[Add a non-null column to table_four called column_four [DECIMAL(9,5)], set to 10.0]",
      "STEPEND:[AddColumnToTableUpgradeStep]",
      "STEPSTART:[DAVEDEV-123]-[AddDslDataChangeStep]-[DSL data change]",
      String.format("DATACHANGE:[Add record into myTable:%n    - Set wibble to 'column1']"),
      "STEPEND:[AddDslDataChangeStep]",
      "VERSIONEND:[ALFA v1.0.0]"
    );

    assertEquals(expected, actual);
  }


  /**
   * A statement consumer which adds any consumed items to an internal list.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static class ListBackedHumanReadableStatementConsumer implements HumanReadableStatementConsumer {

    /**
     * List of strings written to the consumer.
     */
    private final List<String> list = new ArrayList<>();


    /**
     * @see org.alfasoftware.morf.upgrade.HumanReadableStatementConsumer#versionStart(java.lang.String)
     */
    @Override
    public void versionStart(String versionNumber) {
      list.add("VERSIONSTART:[" + versionNumber + "]");
    }


    /**
     * @see org.alfasoftware.morf.upgrade.HumanReadableStatementConsumer#upgradeStepStart(java.lang.String, java.lang.String, String)
     */
    @Override
    public void upgradeStepStart(String name, String description, String jiraId) {
      list.add("STEPSTART:[" + jiraId + "]-[" + name + "]-[" + description + "]");
    }


    /**
     * @see org.alfasoftware.morf.upgrade.HumanReadableStatementConsumer#schemaChange(java.lang.String)
     */
    @Override
    public void schemaChange(String description) {
      list.add("CHANGE:[" + description + "]");
    }


    /**
     * @see org.alfasoftware.morf.upgrade.HumanReadableStatementConsumer#upgradeStepEnd(java.lang.String)
     */
    @Override
    public void upgradeStepEnd(String name) {
      list.add("STEPEND:[" + name + "]");
    }


    /**
     * @see org.alfasoftware.morf.upgrade.HumanReadableStatementConsumer#versionEnd(java.lang.String)
     */
    @Override
    public void versionEnd(String versionNumber) {
      list.add("VERSIONEND:[" + versionNumber + "]");
    }


    /**
     * @see org.alfasoftware.morf.upgrade.HumanReadableStatementConsumer#dataChange(java.lang.String)
     */
    @Override
    public void dataChange(String description) {
      list.add("DATACHANGE:[" + description + "]");
    }


    /**
     * Gets the list of items sent to the consumer.
     *
     * @return the list
     */
    public List<String> getList() {
      return list;
    }

  }
}
