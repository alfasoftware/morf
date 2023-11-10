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

import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.upgrade.ExistingViewStateLoader.Result;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableMap;



/**
 * Tests for {@link ExistingViewStateLoader}.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class TestExistingViewStateLoader {

  private final ExistingViewHashLoader existingViewHashLoader = mock(ExistingViewHashLoader.class, RETURNS_SMART_NULLS);
  private final ViewDeploymentValidator viewDeploymentValidator = mock(ViewDeploymentValidator.class, RETURNS_SMART_NULLS);
  private final SqlDialect dialect = mock(SqlDialect.class, RETURNS_SMART_NULLS);

  private ExistingViewStateLoader onTest;


  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    onTest = new ExistingViewStateLoader(dialect, existingViewHashLoader, viewDeploymentValidator);

    // All hash requests should return the field value + the table name.
    when(dialect.convertStatementToHash(any(SelectStatement.class))).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        SelectStatement statement = (SelectStatement)invocation.getArguments()[0];
        return ((FieldLiteral)statement.getFields().get(0)).getValue() + statement.getTable().getName();
      }
    });

    when(viewDeploymentValidator.validateExistingView(any(View.class), any(UpgradeSchemas.class))).thenReturn(true);
    when(viewDeploymentValidator.validateMissingView(any(View.class), any(UpgradeSchemas.class))).thenReturn(true);
  }


  /**
   * Tests that when we have two matching schemas and matching hashes, there are
   * no changes to deploy, even if there are case differences between the view
   * names in the source and target schemas.
   */
  @Test
  public void testNoChanges() {
    Schema sourceSchema = schema(
      view("View1", select(literal("x")).from(tableRef("a"))),
      view("VIEW2", select(literal("y")).from(tableRef("b"))),
      view("view3", select(literal("z")).from(tableRef("c")))
    );

    Schema targetSchema = schema(
      view("VIEW1", select(literal("x")).from(tableRef("a"))),
      view("view2", select(literal("y")).from(tableRef("b"))),
      view("View3", select(literal("z")).from(tableRef("c")))
    );

    when(existingViewHashLoader.loadViewHashes(sourceSchema)).thenReturn(Optional.<Map<String, String>>of(ImmutableMap.of(
      "VIEW1", "xa",
      "VIEW2", "yb",
      "VIEW3", "zc"
    )));

    assertTrue("No changes", onTest.viewChanges(sourceSchema, targetSchema).isEmpty());
  }


  /**
   * Tests that when we have two matching schemas and matching hashes but no
   * deployed views table, we drop and recreate everything
   */
  @Test
  public void testNoChangesButNoDeployedViewsTable() {
    View sourceView1   = view("View1", select(literal("x")).from(tableRef("a")));
    View sourceView2   = view("VIEW2", select(literal("y")).from(tableRef("b")));
    View sourceView3   = view("view3", select(literal("z")).from(tableRef("c")));
    Schema sourceSchema = schema(sourceView1, sourceView2, sourceView3);

    View targetView1   = view("VIEW1", select(literal("x")).from(tableRef("a")));
    View targetView2   = view("view2", select(literal("y")).from(tableRef("b")));
    View targetView3   = view("View3", select(literal("z")).from(tableRef("c")));
    Schema targetSchema = schema(targetView1, targetView2, targetView3);

    when(existingViewHashLoader.loadViewHashes(sourceSchema)).thenReturn(Optional.<Map<String, String>>empty());

    Result viewChanges = onTest.viewChanges(sourceSchema, targetSchema);

    assertThat(
      "Deploying all target views",
      viewChanges.getViewsToDeploy(),
      containsInAnyOrder(targetView1, targetView2, targetView3)
    );

    assertThat(
      "Dropping all source views",
      viewChanges.getViewsToDrop(),
      containsInAnyOrder(sourceView1, sourceView2, sourceView3)
    );
  }


  /**
   * Tests that when we have two matching schemas, matching hashes and an empty
   * deployed views table, we drop and recreate everything
   */
  @Test
  public void testNoChangesButEmptyDeployedViewsTable() {
    View sourceView1   = view("View1", select(literal("x")).from(tableRef("a")));
    View sourceView2   = view("VIEW2", select(literal("y")).from(tableRef("b")));
    Schema sourceSchema = schema(sourceView1, sourceView2);

    View targetView1   = view("VIEW1", select(literal("x")).from(tableRef("a")));
    View targetView2   = view("view2", select(literal("y")).from(tableRef("b")));
    Schema targetSchema = schema(targetView1, targetView2);

    when(existingViewHashLoader.loadViewHashes(sourceSchema)).thenReturn(Optional.<Map<String, String>>of(ImmutableMap.<String, String>of()));

    Result viewChanges = onTest.viewChanges(sourceSchema, targetSchema);

    assertThat(
      "Deploying all target views",
      viewChanges.getViewsToDeploy(),
      containsInAnyOrder(targetView1, targetView2)
    );

    assertThat(
      "Dropping all source views",
      viewChanges.getViewsToDrop(),
      containsInAnyOrder(sourceView1, sourceView2)
    );
  }


  /**
   * Tests that we drop unnecessary views.
   */
  @Test
  public void testDropUnwantedViews() {
    View sourceView1   = view("View1", select(literal("x")).from(tableRef("a")));
    View sourceView2   = view("VIEW2", select(literal("y")).from(tableRef("b")));
    View sourceViewOld1= view("Old1", select(literal("z")).from(tableRef("c")));
    View sourceViewOld2= view("Old2", select(literal("z")).from(tableRef("c")));
    Schema sourceSchema = schema(sourceView1, sourceView2, sourceViewOld1, sourceViewOld2);

    View targetView1   = view("VIEW1", select(literal("x")).from(tableRef("a")));
    View targetView2   = view("view2", select(literal("y")).from(tableRef("b")));
    Schema targetSchema = schema(targetView1, targetView2);

    when(existingViewHashLoader.loadViewHashes(sourceSchema)).thenReturn(Optional.<Map<String, String>>of(ImmutableMap.of(
      "VIEW1", "xa",
      "VIEW2", "yb"
    )));

    Result viewChanges = onTest.viewChanges(sourceSchema, targetSchema);

    assertThat("Nothing to deploy", viewChanges.getViewsToDeploy(), Matchers.hasSize(0));
    assertThat("Dropping unnecessary view", viewChanges.getViewsToDrop(), containsInAnyOrder(sourceViewOld1, sourceViewOld2));
  }


  /**
   * Tests that we add any views which don't exist.
   */
  @Test
  public void testDeployMissingViews() {
    View sourceView1   = view("View1", select(literal("x")).from(tableRef("a")));
    View sourceView2   = view("VIEW2", select(literal("y")).from(tableRef("b")));
    Schema sourceSchema = schema(sourceView1, sourceView2);

    View targetView1   = view("VIEW1", select(literal("x")).from(tableRef("a")));
    View targetView2   = view("view2", select(literal("y")).from(tableRef("b")));
    View targetViewNew1= view("New1", select(literal("z")).from(tableRef("c")));
    View targetViewNew2= view("New2", select(literal("z")).from(tableRef("c")));
    Schema targetSchema = schema(targetView1, targetView2, targetViewNew1, targetViewNew2);

    when(existingViewHashLoader.loadViewHashes(sourceSchema)).thenReturn(Optional.<Map<String, String>>of(ImmutableMap.of(
      "VIEW1", "xa",
      "VIEW2", "yb"
    )));

    Result viewChanges = onTest.viewChanges(sourceSchema, targetSchema);

    assertThat("Deploy new views", viewChanges.getViewsToDeploy(), containsInAnyOrder(targetViewNew1, targetViewNew2));
    assertThat("Nothing to drop", viewChanges.getViewsToDrop(), Matchers.hasSize(0));
  }


  /**
   * Tests that we add any views which don't exist and clean up orphaned DeployedViews
   * records.
   */
  @Test
  public void testDeployMissingViewsAndCleanUpOrphanedDeployedViewsRecord() {
    View sourceView1   = view("View1", select(literal("x")).from(tableRef("a")));
    View sourceView2   = view("VIEW2", select(literal("y")).from(tableRef("b")));
    Schema sourceSchema = schema(sourceView1, sourceView2);

    View targetView1   = view("VIEW1", select(literal("x")).from(tableRef("a")));
    View targetView2   = view("view2", select(literal("y")).from(tableRef("b")));
    View targetViewNew1= view("New1", select(literal("z")).from(tableRef("c")));
    View targetViewNew2= view("New2", select(literal("z")).from(tableRef("c")));
    Schema targetSchema = schema(targetView1, targetView2, targetViewNew1, targetViewNew2);

    when(existingViewHashLoader.loadViewHashes(sourceSchema)).thenReturn(Optional.<Map<String, String>>of(ImmutableMap.of(
      "VIEW1", "xa",
      "VIEW2", "yb",
      "NEW1", "zc",
      "NEW2", "zc"
    )));

    Result viewChanges = onTest.viewChanges(sourceSchema, targetSchema);

    assertThat("Deploy new views", viewChanges.getViewsToDeploy(), containsInAnyOrder(targetViewNew1, targetViewNew2));
    assertThat("Drop the orphaned DeployedViews records", viewChanges.getViewsToDrop(), containsInAnyOrder(targetViewNew1, targetViewNew2));
  }


  /**
   * Tests that we redeploy views with mismatched hashes.
   */
  @Test
  public void testMismatchedHash() {
    View sourceView1   = view("View1", select(literal("x")).from(tableRef("a")));
    View sourceView2   = view("VIEW2", select(literal("y")).from(tableRef("b")));
    View sourceView3   = view("view3", select(literal("z")).from(tableRef("c")));
    Schema sourceSchema = schema(sourceView1, sourceView2, sourceView3);

    View targetView1   = view("VIEW1", select(literal("CHANGED")).from(tableRef("a")));
    View targetView2   = view("view2", select(literal("y")).from(tableRef("b")));
    View targetView3   = view("View3", select(literal("z")).from(tableRef("CHANGED")));
    Schema targetSchema = schema(targetView1, targetView2, targetView3);

    when(existingViewHashLoader.loadViewHashes(sourceSchema)).thenReturn(Optional.<Map<String, String>>of(ImmutableMap.of(
      "VIEW1", "xa",
      "VIEW2", "yb",
      "VIEW3", "zc"
    )));

    Result viewChanges = onTest.viewChanges(sourceSchema, targetSchema);

    assertThat("Deploy new views", viewChanges.getViewsToDeploy(), containsInAnyOrder(targetView1, targetView3));
    assertThat("Drop the orphaned DeployedViews records", viewChanges.getViewsToDrop(), containsInAnyOrder(sourceView1, sourceView3));
  }
}
