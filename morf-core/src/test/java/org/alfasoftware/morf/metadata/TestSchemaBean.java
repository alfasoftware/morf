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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link SchemaBean}.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestSchemaBean {

  /**
   * Tests that schema bean is case insensitive.
   */
  @Test
  public void testCaseSensitivity() {
    Schema schema = new SchemaBean(new MockSchema());

    // All of these tables should exist
    assertTrue("Upper case", schema.tableExists("TABLE1"));
    assertTrue("Lower case", schema.tableExists("table1"));
    assertTrue("Mixed case", schema.tableExists("Table1"));

    // All of these views should exist
    assertTrue("Upper case", schema.viewExists("VIEW1"));
    assertTrue("Lower case", schema.viewExists("view1"));
    assertTrue("Mixed case", schema.viewExists("View1"));

    // All of these sequences should exist
    assertTrue("Upper case", schema.sequenceExists("SEQUENCE1"));
    assertTrue("Lower case", schema.sequenceExists("sequence1"));
    assertTrue("Mixed case", schema.sequenceExists("Sequence1"));

    // Table is found
    Table table = schema.getTable("TABLE1");
    assertEquals("Table returned", "Table1", table.getName());

    // Check this twice to ensure we are not reading the source schema again.
    assertNotNull("Columns first call", table.columns());
    assertNotNull("Columns second call", table.columns());
  }


  /**
   * Provides source schema data for {@link SchemaBean} to read.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private class MockSchema implements Schema {

    // -- Track which methods have been called...
    //
    private boolean tableCalled;
    private boolean viewCalled;
    private boolean sequenceCalled;

    // -- Track which methods have been called...
    //
    private boolean tableNamesCalled;
    private boolean viewNamesCalled;
    private boolean sequenceNamesCalled;

    /**
     * Table for our mock schema.
     */
    private final Table table = new Table() {

      /** Track which methods have been called */
      private boolean columnsCalled;

      /** Track which methods have been called */
      private boolean nameCalled;

      /** Holds all the columns in our mock table. */
      private final List<Column> columns = ImmutableList.<Column>of(
        column("column1", DataType.STRING).nullable(),
        column("column2", DataType.STRING).nullable()
      );

      @Override
      public String getName() {
        if (nameCalled) {
          fail("Source schema should only be read once");
        }
        nameCalled = true;
        return "Table1";
      }

      @Override
      public List<Index> indexes() {
        return new ArrayList<Index>();
      }

      @Override
      public List<Column> columns() {
        if (columnsCalled) {
          fail("Source schema should only be read once");
        }
        columnsCalled = true;
        return columns;
      }

      @Override
      public boolean isTemporary() {
        return false;
      }
    };


    /**
     * View for our schema.
     */
    private final View view = new View() {
      @Override public String getName() {
        return "View1";
      }

      @Override public SelectStatement getSelectStatement() {
        return select(field("column1")).from(new TableReference("Table1"));
      }

      @Override
      public boolean knowsSelectStatement() {
        return true;
      }

      @Override
      public String[] getDependencies() {
        String[] array = {"View2"};
        return array;
      }

      @Override
      public boolean knowsDependencies() {
        return true;
      }
    };


    /**
     * Sequence for our schema.
     */
    private final Sequence sequence = new Sequence() {
      @Override public String getName() {
        return "Sequence1";
      }

      @Override
      public boolean knowsStartsWith() { return true; }

      @Override
      public Integer getStartsWith() {
        return 1;
      }

      @Override
      public boolean isTemporary() {
        return false;
      }
    };


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
     */
    @Override
    public Table getTable(String name) {
      if (tableCalled) {
        fail("Table definition from source schema should only be read once");
      }

      if (name.equalsIgnoreCase(table.getName())) {
        tableCalled = true;
        return table;
      } else {
        throw new IllegalArgumentException("Table not known");
      }
    }


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
     */
    @Override
    public boolean isEmptyDatabase() {
      return false;
    }


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
     */
    @Override
    public boolean tableExists(String name) {
      return name.equalsIgnoreCase(table.getName());
    }


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.Schema#tableNames()
     */
    @Override
    public Collection<String> tableNames() {
      if (tableNamesCalled) {
        fail("Table names on source schema should only be read once");
      }

      tableNamesCalled = true;
      return Arrays.asList(table.getName());
    }


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.Schema#tables()
     */
    @Override
    public Collection<Table> tables() {
      return ImmutableList.of(table);
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#viewExists(java.lang.String)
     */
    @Override
    public boolean viewExists(String name) {
      return name.equalsIgnoreCase(view.getName());
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#getView(java.lang.String)
     */
    @Override
    public View getView(String name) {
      if (viewCalled) {
        fail("View definition from source schema should only be read once");
      }

      if (name.equalsIgnoreCase(view.getName())) {
        viewCalled = true;
        return view;
      } else {
        throw new IllegalArgumentException("View not known");
      }
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#viewNames()
     */
    @Override
    public Collection<String> viewNames() {
      if (viewNamesCalled) {
        fail("View names on source schema should only be read once");
      }

      viewNamesCalled = true;
      return Arrays.asList(view.getName());
    }

    /**
     * @see org.alfasoftware.morf.metadata.Schema#views()
     */
    @Override
    public Collection<View> views() {
      return ImmutableList.of(view);
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#sequenceExists(String)
     */
    @Override
    public boolean sequenceExists(String name) {
      return name.equalsIgnoreCase(sequence.getName());
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#getSequence(String)
     */
    @Override
    public Sequence getSequence(String name) {
      if (sequenceCalled) {
        fail("Sequence definition from source schema should only be read once");
      }

      if (name.equalsIgnoreCase(sequence.getName())) {
        sequenceCalled = true;
        return sequence;
      } else {
        throw new IllegalArgumentException("Sequence not known");
      }
    }


    /**
     * @see Schema#sequenceNames()
     */
    @Override
    public Collection<String> sequenceNames() {
      if (sequenceNamesCalled) {
        fail("Sequence names on source schema should only be read once");
      }

      sequenceNamesCalled = true;
      return Arrays.asList(sequence.getName());
    }


    /**
     * @see Schema#sequences()
     */
    @Override
    public Collection<Sequence> sequences() {
      return ImmutableList.of(sequence);
    }
  }
}
