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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeployedIndexesModelEnricher} (slim invariant).
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexesModelEnricherImpl {

  private DeployedIndexesDAO dao;
  private DeferredIndexSession session;
  private UpgradeConfigAndContext config;

  @Before
  public void setUp() {
    dao = mock(DeployedIndexesDAO.class);
    session = new DeferredIndexSessionImpl();
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
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input, session);

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
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input, session);

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
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input, session);

    // then
    assertSame(input, result.getSchema());
  }


  /** Deferred index with no physical counterpart (status != COMPLETED) is added to the schema as
   *  a virtual entry, and the state records it as absent. */
  @Test
  public void testDeferredIndexAddedAsVirtualAndStateRecordsAbsent() {
    // given — table with no physical indexes, tracking row says PENDING
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(), column("name", DataType.STRING, 50))
    );
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName("MyTable");
    entry.setIndexName("MyIdx");
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("name"));
    entry.setStatus(DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input, session);

    // then — virtual deferred index appears in schema
    assertEquals(1, result.getSchema().getTable("MyTable").indexes().size());
    Index virtual = result.getSchema().getTable("MyTable").indexes().get(0);
    assertEquals("MyIdx", virtual.getName());
    assertTrue("Should be deferred", virtual.isDeferred());
    // and — state records physical absence
    assertEquals("State should record ABSENT",
        IndexPresence.ABSENT, result.getState().getPresence("MyTable", "MyIdx"));
  }


  /** Slim invariant: COMPLETED tracking rows are not virtualized — the index is already
   *  in the physical schema; no state entry is needed (UNKNOWN default). */
  @Test
  public void testCompletedEntryIsNotVirtualized() {
    // given — tracking row is COMPLETED (deferred but now physically built)
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("MyIdx").columns("id").deferred())
    );
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName("MyTable");
    entry.setIndexName("MyIdx");
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    EnrichedModel result = enricher.enrich(input, session);

    // then — schema unchanged (physical already has it), state has no entry
    assertSame(input, result.getSchema());
    assertEquals("Non-virtualized index yields no state entry (UNKNOWN default)",
        IndexPresence.UNKNOWN, result.getState().getPresence("MyTable", "MyIdx"));
  }


  /** Enricher primes the service with every persisted row so visitor operations
   *  against prior-upgrade deferred rows emit correct DML. */
  @Test
  public void testEnrichPrimesServiceWithEveryPersistedRow() {
    // given — two persisted rows; service is spied so prime() calls can be verified
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("TableA").columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("TableB").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
    );
    DeployedIndex entryA = new DeployedIndex();
    entryA.setTableName("TableA");
    entryA.setIndexName("A_Idx");
    entryA.setIndexUnique(false);
    entryA.setIndexColumns(List.of("id"));
    entryA.setStatus(DeployedIndexStatus.COMPLETED);
    DeployedIndex entryB = new DeployedIndex();
    entryB.setTableName("TableB");
    entryB.setIndexName("B_Idx");
    entryB.setIndexUnique(false);
    entryB.setIndexColumns(List.of("name"));
    entryB.setStatus(DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entryA, entryB));
    DeferredIndexSession spy = mock(DeferredIndexSession.class);
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    enricher.enrich(input, spy);

    // then — every persisted row primes the session exactly once
    verify(spy).prime(entryA);
    verify(spy).prime(entryB);
  }


  /** After priming, the primed service can answer isTracked / isTrackedDeferred
   *  for persisted deferred rows — the visitor depends on this to know whether
   *  remove/rename operations should emit DML. */
  @Test
  public void testPrimingEnablesIsTrackedChecks() {
    // given
    org.alfasoftware.morf.metadata.Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
    );
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName("MyTable");
    entry.setIndexName("MyIdx");
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("name"));
    entry.setStatus(DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    enricher.enrich(input, session);

    // then — service is populated; visitor can now operate on this persisted row
    assertTrue("Persisted row should be tracked in service after priming",
        session.isTrackedDeferred("MyTable", "MyIdx"));
    assertTrue("Persisted deferred row should read as deferred after priming",
        session.isTrackedDeferred("MyTable", "MyIdx"));
  }
}
