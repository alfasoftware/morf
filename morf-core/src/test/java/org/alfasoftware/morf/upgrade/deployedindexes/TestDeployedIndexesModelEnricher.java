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
import org.alfasoftware.morf.upgrade.deployedindexes.EnrichedModel;
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
    EnrichedModel result = enricher.enrich(input);

    // then
    assertSame(input, result.getSchema());
    assertEquals("Empty state should report UNKNOWN for any index",
        IndexPresence.UNKNOWN, result.getState().getPresence("Foo", "Any"));
  }


  /** When DeployedIndexes table doesn't exist, returns input schema unchanged and empty state. */
  @Test
  public void testNoDeployedIndexesTableReturnsUnchanged() {
    // given
    org.alfasoftware.morf.metadata.Schema input =
        schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input);

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
    EnrichedModel result = enricher.enrich(input);

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
    EnrichedModel result = enricher.enrich(input);

    // then -- schema carries deferred flag
    Index rebuilt = result.getSchema().getTable("MyTable").indexes().get(0);
    assertTrue("Deferred flag should be propagated from tracking row", rebuilt.isDeferred());
    // and -- state records physical presence
    assertEquals("State should record PRESENT",
        IndexPresence.PRESENT, result.getState().getPresence("MyTable", "MyIdx"));
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
    EnrichedModel result = enricher.enrich(input);

    // then -- virtual deferred index appears in schema
    assertEquals(1, result.getSchema().getTable("MyTable").indexes().size());
    Index virtual = result.getSchema().getTable("MyTable").indexes().get(0);
    assertEquals("MyIdx", virtual.getName());
    assertTrue("Should be deferred", virtual.isDeferred());
    // and -- state records physical absence
    assertEquals("State should record ABSENT",
        IndexPresence.ABSENT, result.getState().getPresence("MyTable", "MyIdx"));
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
    EnrichedModel result = enricher.enrich(input);

    // then
    assertTrue("PRF index should pass through", result.getSchema().getTable("MyTable").indexes().stream()
        .anyMatch(i -> "MyTable_PRF1".equals(i.getName())));
  }


  // ---- Rebuild preserves index properties -------------------------------

  /** rebuildIndex preserves isUnique and multi-column ordering. */
  @Test
  public void testRebuildPreservesUniqueAndColumnOrder() {
    // given -- physical index is unique and multi-column; tracking says not deferred
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("UniqueIdx").unique().columns("a", "b", "c"))
    );
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName("MyTable");
    entry.setIndexName("UniqueIdx");
    entry.setIndexDeferred(false);
    entry.setIndexUnique(true);
    entry.setIndexColumns(List.of("a", "b", "c"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input);

    // then -- rebuilt index is unique, columns in declared order, not deferred
    Index rebuilt = result.getSchema().getTable("MyTable").indexes().get(0);
    assertTrue("isUnique should be preserved", rebuilt.isUnique());
    assertEquals(List.of("a", "b", "c"), rebuilt.columnNames());
    assertFalse("isDeferred should be false", rebuilt.isDeferred());
  }


  /** Non-deferred tracking row + matching physical: state records PRESENT. */
  @Test
  public void testNonDeferredPhysicalRecordsPresent() {
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
    entry.setIndexDeferred(false);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input);

    // then -- rebuilt index is not deferred, state is PRESENT
    assertFalse(result.getSchema().getTable("MyTable").indexes().get(0).isDeferred());
    assertEquals(IndexPresence.PRESENT, result.getState().getPresence("MyTable", "MyIdx"));
  }


  /** Mixed physical + virtual-deferred on one table: both appear in schema, state
   *  records PRESENT for the physical one and ABSENT for the virtual. */
  @Test
  public void testMixedPhysicalAndVirtualOnOneTable() {
    // given -- physical Idx1 + tracking row Idx1 + tracking row Idx2 (deferred, no physical)
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(), column("name", DataType.STRING, 50))
            .indexes(index("Idx1").columns("id"))
    );
    DeployedIndex physicalEntry = new DeployedIndex();
    physicalEntry.setTableName("MyTable");
    physicalEntry.setIndexName("Idx1");
    physicalEntry.setIndexDeferred(false);
    physicalEntry.setIndexUnique(false);
    physicalEntry.setIndexColumns(List.of("id"));
    physicalEntry.setStatus(DeployedIndexStatus.COMPLETED);
    DeployedIndex virtualEntry = new DeployedIndex();
    virtualEntry.setTableName("MyTable");
    virtualEntry.setIndexName("Idx2");
    virtualEntry.setIndexDeferred(true);
    virtualEntry.setIndexUnique(false);
    virtualEntry.setIndexColumns(List.of("name"));
    virtualEntry.setStatus(DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(physicalEntry, virtualEntry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input);

    // then -- schema has both indexes; state distinguishes them
    assertEquals(2, result.getSchema().getTable("MyTable").indexes().size());
    assertEquals(IndexPresence.PRESENT, result.getState().getPresence("MyTable", "Idx1"));
    assertEquals(IndexPresence.ABSENT, result.getState().getPresence("MyTable", "Idx2"));
  }


  /** Multiple tables in a single enrich call — each is enriched independently. */
  @Test
  public void testMultipleTables() {
    // given -- two tables, each with its own physical index tracked in DeployedIndexes
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("TableA").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("A_Idx").columns("id")),
        table("TableB").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("B_Idx").columns("id"))
    );
    DeployedIndex ea = new DeployedIndex();
    ea.setTableName("TableA");
    ea.setIndexName("A_Idx");
    ea.setIndexDeferred(true);
    ea.setIndexUnique(false);
    ea.setIndexColumns(List.of("id"));
    ea.setStatus(DeployedIndexStatus.COMPLETED);
    DeployedIndex eb = new DeployedIndex();
    eb.setTableName("TableB");
    eb.setIndexName("B_Idx");
    eb.setIndexDeferred(false);
    eb.setIndexUnique(false);
    eb.setIndexColumns(List.of("id"));
    eb.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(ea, eb));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input);

    // then -- deferred flag from tracking row propagates per-table
    assertTrue(result.getSchema().getTable("TableA").indexes().get(0).isDeferred());
    assertFalse(result.getSchema().getTable("TableB").indexes().get(0).isDeferred());
    assertEquals(IndexPresence.PRESENT, result.getState().getPresence("TableA", "A_Idx"));
    assertEquals(IndexPresence.PRESENT, result.getState().getPresence("TableB", "B_Idx"));
  }


  /** Orphan tracking row (table not in physical schema and not a Morf table)
   *  is tolerated — logs a warning, doesn't throw. */
  /**
   * An orphan tracking row referencing a missing table now hard-fails. P1.2
   * tightened the policy from log-warn to throw: in a correctly-managed Morf
   * database orphans cannot be produced by normal operation, so surfacing
   * them as an error is the right default.
   */
  @Test(expected = IllegalStateException.class)
  public void testOrphanRowForMissingTableThrows() {
    // given -- DeployedIndexes references GoneTable which isn't in the physical schema
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("ExistingTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
    );
    DeployedIndex orphan = new DeployedIndex();
    orphan.setTableName("GoneTable");
    orphan.setIndexName("OrphanIdx");
    orphan.setIndexDeferred(true);
    orphan.setIndexUnique(false);
    orphan.setIndexColumns(List.of("c"));
    orphan.setStatus(DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(orphan));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when -- throws because the orphan row is a schema inconsistency
    enricher.enrich(input);
  }


  /**
   * Morf infrastructure tables (UpgradeAudit, DeployedViews, DeployedIndexes)
   * are no longer skipped during enrichment. Their indexes are enriched the
   * same way as user tables: a physical index must have a matching tracking
   * row, else consistency validation throws.
   */
  @Test
  public void testMorfInfrastructureTableIndexIsEnriched() {
    // given -- UpgradeAudit is in the physical schema with a tracked index
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("UpgradeAudit_1").columns("id"))
    );
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME);
    entry.setIndexName("UpgradeAudit_1");
    entry.setIndexDeferred(false);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input);

    // then -- the Morf-table index is recorded as PRESENT (not skipped)
    assertEquals(IndexPresence.PRESENT,
        result.getState().getPresence(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME, "UpgradeAudit_1"));
  }


  /**
   * A physical index on a Morf infrastructure table without a tracking row
   * triggers consistency validation — no Morf-table skip.
   */
  @Test(expected = IllegalStateException.class)
  public void testUntrackedPhysicalIndexOnMorfTableThrows() {
    // given -- DeployedViews has a physical index but no tracking row
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("DeployedViews_1").columns("id"))
    );
    // Need at least one non-orphan tracking row so enrich doesn't skip via empty-table fast path
    DeployedIndex otherEntry = new DeployedIndex();
    otherEntry.setTableName(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME);
    otherEntry.setIndexName("DeployedIdx_placeholder");
    otherEntry.setIndexDeferred(false);
    otherEntry.setIndexUnique(false);
    otherEntry.setIndexColumns(List.of("id"));
    otherEntry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(otherEntry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when -- throws: DeployedViews_1 exists physically but no tracking row
    enricher.enrich(input);
  }
}
