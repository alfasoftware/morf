/* Copyright 2026 Alfa Financial Software
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

package org.alfasoftware.morf.upgrade.deployedindexes;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesModelEnricher.Result;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeployedIndexesModelEnricher}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexesModelEnricher {

  private DeployedIndexesDAO dao;
  private UpgradeConfigAndContext config;

  @Before
  public void setUp() {
    dao = mock(DeployedIndexesDAO.class);
    config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
  }


  /** When feature is disabled, enrich returns input schema unchanged and empty state. */
  @Test
  public void testDisabledReturnsInputUnchanged() {
    // given
    config.setDeferredIndexCreationEnabled(false);
    org.alfasoftware.morf.metadata.Schema input =
        schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Result result = enricher.enrich(input);

    // then
    assertSame(input, result.getSchema());
    assertFalse("Empty state should report no known presence",
        result.getState().isKnownPhysicallyPresent("Foo", "Any"));
    assertFalse("Empty state should report no known absence",
        result.getState().isKnownPhysicallyAbsent("Foo", "Any"));
  }


  /** When DeployedIndexes table doesn't exist, returns input schema unchanged and empty state. */
  @Test
  public void testNoDeployedIndexesTableReturnsUnchanged() {
    // given
    org.alfasoftware.morf.metadata.Schema input =
        schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Result result = enricher.enrich(input);

    // then
    assertSame(input, result.getSchema());
  }


  /** When DeployedIndexes table is empty, returns input schema unchanged and empty state. */
  @Test
  public void testEmptyDeployedIndexesReturnsUnchanged() {
    // given
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("Foo_1").columns("id"))
    );
    when(dao.findAll()).thenReturn(Collections.emptyList());
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Result result = enricher.enrich(input);

    // then
    assertSame(input, result.getSchema());
  }


  /** Physical index with a matching DeployedIndexes row has its deferred flag propagated,
   *  and the state records it as physically present. */
  @Test
  public void testPhysicalIndexCarriesDeferredFlagAndStateRecordsPresent() {
    // given
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("MyIdx").columns("id"))
    );
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName("MyTable");
    entry.setIndexName("MyIdx");
    entry.setIndexDeferred(true);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Result result = enricher.enrich(input);

    // then -- schema carries deferred flag
    Index rebuilt = result.getSchema().getTable("MyTable").indexes().get(0);
    assertTrue("Deferred flag should be propagated from tracking row", rebuilt.isDeferred());
    // and -- state records physical presence
    assertTrue("State should record physical presence",
        result.getState().isKnownPhysicallyPresent("MyTable", "MyIdx"));
    assertFalse("State should not record absence",
        result.getState().isKnownPhysicallyAbsent("MyTable", "MyIdx"));
  }


  /** Deferred index with no physical counterpart is added to the schema as a virtual entry,
   *  and the state records it as absent. */
  @Test
  public void testDeferredIndexAddedAsVirtualAndStateRecordsAbsent() {
    // given -- table with no physical indexes
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(), column("name", DataType.STRING, 50))
    );
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName("MyTable");
    entry.setIndexName("MyIdx");
    entry.setIndexDeferred(true);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("name"));
    entry.setStatus(DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Result result = enricher.enrich(input);

    // then -- virtual deferred index appears in schema
    assertEquals(1, result.getSchema().getTable("MyTable").indexes().size());
    Index virtual = result.getSchema().getTable("MyTable").indexes().get(0);
    assertEquals("MyIdx", virtual.getName());
    assertTrue("Should be deferred", virtual.isDeferred());
    // and -- state records physical absence
    assertTrue("State should record physical absence",
        result.getState().isKnownPhysicallyAbsent("MyTable", "MyIdx"));
    assertFalse("State should not record presence",
        result.getState().isKnownPhysicallyPresent("MyTable", "MyIdx"));
  }


  /** Non-deferred index missing from DB should throw error. */
  @Test(expected = IllegalStateException.class)
  public void testNonDeferredMissingFromDbThrowsError() {
    // given -- DeployedIndexes says non-deferred index exists, but it's not in the physical schema
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
    );
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName("MyTable");
    entry.setIndexName("MissingIdx");
    entry.setIndexDeferred(false);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when -- should throw
    enricher.enrich(input);
  }


  /** Physical index not tracked in DeployedIndexes should throw error. */
  @Test(expected = IllegalStateException.class)
  public void testUntrackedPhysicalIndexThrowsError() {
    // given -- physical index exists but no DeployedIndexes row
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("UntrackedIdx").columns("id"))
    );
    // DAO returns entry for a DIFFERENT index
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName("MyTable");
    entry.setIndexName("OtherIdx");
    entry.setIndexDeferred(false);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when -- should throw
    enricher.enrich(input);
  }


  /** _PRF indexes should be excluded from validation. */
  @Test
  public void testPrfIndexExcludedFromValidation() {
    // given -- _PRF index in physical schema with no DeployedIndexes row
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("MyTable_PRF1").columns("id"))
    );
    when(dao.findAll()).thenReturn(Collections.emptyList());
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when -- should NOT throw despite untracked PRF index
    Result result = enricher.enrich(input);

    // then
    assertTrue("PRF index should pass through", result.getSchema().getTable("MyTable").indexes().stream()
        .anyMatch(i -> "MyTable_PRF1".equals(i.getName())));
  }
}
