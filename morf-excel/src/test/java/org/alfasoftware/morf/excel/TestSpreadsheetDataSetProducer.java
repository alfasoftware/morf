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

package org.alfasoftware.morf.excel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.dataset.Record;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Ensure that the {@link SpreadsheetDataSetProducer} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestSpreadsheetDataSetProducer {

  /**
   * Ensure that the schema can be retrieved from a set of excel files
   *
   * @throws URISyntaxException ...
   */
  @Test
  public void testGetSchema() throws URISyntaxException {
    final SpreadsheetDataSetProducer producer = produceTestSpreadsheet();

    // Check that the table names were picked up correctly
    Collection<String> tableNames = producer.getSchema().tableNames();
    assertEquals("Number of tables found [" + tableNames + "]", 12, tableNames.size());
    assertTrue("Tables correctly populated [" + tableNames + "]", tableNames.contains("AssetType"));
    assertTrue("Tables correctly populated [" + tableNames + "]", tableNames.contains("Allowance"));
  }


  /**
   * Ensure that the rows can be retrieved from a set of excel files
   *
   * @throws URISyntaxException ...
   */
  @Test
  public void testGetRecords() throws URISyntaxException {
    final SpreadsheetDataSetProducer producer = produceTestSpreadsheet();

    // Check that the table names were picked up correctly
    List<Record> records = Lists.newArrayList(producer.records("UsageMeterType"));
    assertEquals("Number of rows [" + records + "]", 2, records.size());
    Record record = records.get(0);
    assertEquals("Usage Meter", "KM", record.getValue("usageMeter"));
    assertEquals("Description", "Kilometers", record.getValue("description"));
    assertEquals("Usage Meter Type", "M", record.getValue("usageMeterType"));

    record = records.get(1);
    assertEquals("Usage Meter", "HOUR", record.getValue("usageMeter"));
    assertEquals("Description", "Hours", record.getValue("description"));
    assertEquals("Usage Meter Type", "", record.getValue("usageMeterType"));
  }


  /**
   * Ensure that the generated schema behaves as expected
   *
   * @throws URISyntaxException ...
   */
  @Test
  public void testGetSchemaMethods() throws URISyntaxException {
    final SpreadsheetDataSetProducer producer = produceTestSpreadsheet();
    try {
      // Getting the metadata of a table is an unsupported method of the
      // minimal schema
      producer.getSchema().getTable("Name");
      fail("Exception should have been thrown");
    } catch (UnsupportedOperationException e) {
      // This is expected
    }

    try {
      // Getting all tables is an unsupported method of the minimal schema
      producer.getSchema().tables();
      fail("Exception should have been thrown");
    } catch (UnsupportedOperationException e) {
      // This is expected
    }

    assertFalse("Schema is not empty", producer.getSchema().isEmptyDatabase());
    assertTrue("UsageMeterType table exists", producer.getSchema().tableExists("UsageMeterType"));
  }


  private SpreadsheetDataSetProducer produceTestSpreadsheet() {
    InputStream testExcel = getClass().getResourceAsStream("TestSpreadsheetDataSetProducer.xls");
    final SpreadsheetDataSetProducer producer = new SpreadsheetDataSetProducer(testExcel);
    return producer;
  }
}
