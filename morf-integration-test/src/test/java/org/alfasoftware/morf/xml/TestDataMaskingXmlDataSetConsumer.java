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
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.DataSetConsumer.CloseState;
import org.alfasoftware.morf.dataset.MockRecord;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Tests for {@linkplain DataMaskingXmlDataSetConsumer}.
 *
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class TestDataMaskingXmlDataSetConsumer {

  private static Map<String, Set<String>> toMask = Maps.newHashMap();
  static {
    toMask.put("Test", Sets.newHashSet("bar"));
  }

  /**
   * Ensure that get value returns the expected result.
   */
  @Test
  public void getValue() {
    DummyXmlOutputStreamProvider dummyXmlOutputStreamProvider = new DummyXmlOutputStreamProvider();
    DataSetConsumer testConsumer = new DataMaskingXmlDataSetConsumer(dummyXmlOutputStreamProvider, toMask);

    Table metaData = table("Test").columns(
        column("id", DataType.BIG_INTEGER, 19, 0).primaryKey().autoNumbered(123),
        versionColumn(),
        column("bar", DataType.STRING, 10).nullable(),
        column("baz", DataType.STRING, 10).nullable(),
        column("bob", DataType.DECIMAL, 13, 2).nullable()
      ).indexes(
        index("fizz").unique().columns("bar", "baz")
      );

    testConsumer.open();
    List<Record> mockRecords = new ArrayList<Record>();
    mockRecords.add(new MockRecord(metaData, "1", "1", "abc", "123", "456.78"));
    testConsumer.table(metaData, mockRecords);
    testConsumer.close(CloseState.COMPLETE);

    assertEquals("Serialised data set", SourceXML.MASKED_SAMPLE, dummyXmlOutputStreamProvider.getXmlString().trim());
  }

}
