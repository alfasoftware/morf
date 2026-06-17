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

package org.alfasoftware.morf.metadata;

import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.sequence;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Test {@link CompositeSchema} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestCompositeSchema {

  private final Schema schema1 = new SchemaBean(ImmutableList.<Table>of(table("Bobby"), table("Foo"), table("Bar")),
                                                ImmutableList.of(view("View1", select())),
                                                ImmutableList.of(sequence("Sequence1")));
  private final Schema schema2 = new SchemaBean(table("BAR"), table("Baz"));


  /**
   * Test the main mechanism for constructing a composite schema.
   */
  @Test
  public void testDirectly() {
    assertComposite(new CompositeSchema(schema1, schema2));
  }


  /**
   * {@link SchemaUtils#schema(Schema...)} is a convenience method for instantiating
   * composite schema, use it.
   */
  @Test
  public void testHelperMethod() {
    assertComposite(schema(schema1, schema2));
  }


  /**
   * Test that an empty schema is handled.
   */
  @Test
  public void testNoSchema() {
    Schema schema = new CompositeSchema();
    assertTrue("Empty database", schema.isEmptyDatabase());
    assertFalse("Table exists", schema.tableExists("Foo"));
  }


  /**
   * Test that an empty database is handled.
   */
  @Test
  public void testEmptyDatabase() {
    Schema schema = new CompositeSchema(schema(), schema());
    assertTrue("Empty database", schema.isEmptyDatabase());
    assertFalse("Table exists", schema.tableExists("Foo"));
  }


  /**
   * Check that the first schema takes priority for table definitions.
   */
  @Test
  public void testOrdering() {
    assertSame("Bar table", schema1.getTable("Bar"), new CompositeSchema(schema1, schema2).getTable("Bar"));
  }


  /**
   * Assert aspects of {@link Schema} based on {@link #schema1} and {@link #schema2}.
   *
   * @param schema Schema to compare.
   */
  private void assertComposite(Schema schema) {
    Set<String> tableNames = ImmutableSet.of("Bobby", "Foo", "Bar", "Baz");
    Set<String> viewNames  = ImmutableSet.of("View1");
    Set<String> sequenceNames  = ImmutableSet.of("Sequence1");

    assertEquals("tableNames", tableNames, new HashSet<String>(schema.tableNames()));
    assertEquals("tables' size " + schema.tableNames(), tableNames.size(), schema.tables().size());

    assertEquals("viewNames", viewNames, new HashSet<String>(schema.viewNames()));
    assertEquals("views' size " + schema.viewNames(), viewNames.size(), schema.views().size());

    assertEquals("sequenceName", sequenceNames, new HashSet<String>(schema.sequenceNames()));
    assertEquals("sequences' size " + schema.sequences(), sequenceNames.size(), schema.sequences().size());

    assertFalse("Empty", schema.isEmptyDatabase());
    for (String name : tableNames) {
      assertEquals("getTable - " + name, name, schema.getTable(name).getName());
      assertTrue("tableExists - " + name, schema.tableExists(name));
    }

    for (String name : viewNames) {
      assertEquals("getView - " + name, name, schema.getView(name).getName());
      assertTrue("viewExists - " + name, schema.viewExists(name));
    }

    for (String name : sequenceNames) {
      assertEquals("getSequence - " + name, name, schema.getSequence(name).getName());
      assertTrue("sequenceExists - " + name, schema.sequenceExists(name));
    }
  }
}
