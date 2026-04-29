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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeployedIndexesModelEnricher} (row-existence model).
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
    session = new DeferredIndexSessionImpl(new DeployedIndexesStatements());
    config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
  }


  /** When feature is disabled, enrich returns input schema unchanged. */
  @Test
  public void testDisabledReturnsInputUnchanged() {
    // given
    config.setDeferredIndexCreationEnabled(false);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    Schema result = enricher.enrich(input, session);

    // then
    assertSame(input, result);
  }


  /** When DeployedIndexes table doesn't exist, returns input schema unchanged. */
  @Test
  public void testNoDeployedIndexesTableReturnsUnchanged() {
    // given
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    Schema result = enricher.enrich(input, session);

    // then
    assertSame(input, result);
  }


  /** When DeployedIndexes table is empty, returns input schema unchanged. */
  @Test
  public void testEmptyDeployedIndexesReturnsUnchanged() {
    // given
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("Foo_1").columns("id"))
    );
    when(dao.findAll()).thenReturn(Collections.emptyList());
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    Schema result = enricher.enrich(input, session);

    // then
    assertSame(input, result);
  }


  /** Non-COMPLETED row with no physical match → virtualized as deferred,
   *  session marks it as awaiting build. */
  @Test
  public void testUnbuiltDeferredVirtualizedAsDeferred() {
    // given — table with no physical indexes, tracking row says PENDING
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
    );
    DeployedIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    Schema result = enricher.enrich(input, session);

    // then — virtual deferred index appears in schema
    assertEquals(1, result.getTable("MyTable").indexes().size());
    Index virtual = result.getTable("MyTable").indexes().get(0);
    assertEquals("MyIdx", virtual.getName());
    assertTrue("Should be deferred", virtual.isDeferred());
    // and — session sees it as awaiting build
    assertTrue(session.isAwaitingBuild("MyTable", "MyIdx"));
  }


  /** COMPLETED row + matching physical → physical index rebuilt with .deferred()
   *  in enriched schema; session sees it as NOT awaiting (built). */
  @Test
  public void testCompletedDeferredRebuiltWithDeferredFlag() {
    // given — physical index exists, tracking row says COMPLETED
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
            .indexes(index("MyIdx").columns("name"))  // physical, NOT marked deferred
    );
    DeployedIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    Schema result = enricher.enrich(input, session);

    // then — index in enriched schema is now marked deferred
    Index enriched = result.getTable("MyTable").indexes().get(0);
    assertEquals("MyIdx", enriched.getName());
    assertTrue("Built deferred should be reported isDeferred()=true after enrichment",
        enriched.isDeferred());
    // and — session knows it's tracked but NOT awaiting build (status=COMPLETED)
    assertTrue(session.isTrackedDeferred("MyTable", "MyIdx"));
    assertFalse("Built deferred should NOT be awaiting build",
        session.isAwaitingBuild("MyTable", "MyIdx"));
  }


  /** COMPLETED row + NO physical match → drift, throws IllegalStateException. */
  @Test
  public void testCompletedRowWithoutPhysicalMatchThrowsDrift() {
    // given — tracking row says COMPLETED but physical index is missing
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            // no physical MyIdx
    );
    DeployedIndex entry = makeRow("MyTable", "MyIdx", List.of("id"), DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when / then
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> enricher.enrich(input, session));
    assertTrue("Message should mention the missing index",
        ex.getMessage().contains("MyIdx"));
    assertTrue("Message should mention COMPLETED",
        ex.getMessage().contains("COMPLETED"));
  }


  /** Non-COMPLETED row + matching physical → drift, throws IllegalStateException
   *  (tracker thinks it's not built but it IS — adopter probably crashed
   *  between CREATE INDEX and markCompleted). */
  @Test
  public void testNonCompletedRowWithPhysicalMatchThrowsDrift() {
    // given — physical index exists but tracking row says PENDING
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
            .indexes(index("MyIdx").columns("name"))
    );
    DeployedIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when / then
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> enricher.enrich(input, session));
    assertTrue("Message should mention the index",
        ex.getMessage().contains("MyIdx"));
    assertTrue("Message should mention PENDING status",
        ex.getMessage().contains("PENDING"));
  }


  /** Row references a table not in the physical schema → throws IllegalStateException. */
  @Test
  public void testRowReferencingMissingTableThrowsDrift() {
    // given
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey())
            // no other tables
    );
    DeployedIndex entry = makeRow("Ghost", "GhostIdx", List.of("id"), DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when / then
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> enricher.enrich(input, session));
    assertTrue("Message should mention the orphan table",
        ex.getMessage().contains("Ghost"));
  }


  /** Enricher primes the session with every persisted row regardless of status. */
  @Test
  public void testEnrichPrimesSessionWithEveryPersistedRow() {
    // given — two persisted rows, one COMPLETED one PENDING
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("TableA").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("A_Idx").columns("id")),
        table("TableB").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
    );
    DeployedIndex entryA = makeRow("TableA", "A_Idx", List.of("id"), DeployedIndexStatus.COMPLETED);
    DeployedIndex entryB = makeRow("TableB", "B_Idx", List.of("name"), DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entryA, entryB));
    DeferredIndexSession mockSession = mock(DeferredIndexSession.class);
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricherImpl(dao, config);

    // when
    enricher.enrich(input, mockSession);

    // then — both rows primed
    verify(mockSession).prime(entryA);
    verify(mockSession).prime(entryB);
  }


  // ---- helpers --------------------------------------------------------------

  private static DeployedIndex makeRow(String table, String idx, List<String> cols,
                                        DeployedIndexStatus status) {
    DeployedIndex entry = new DeployedIndex();
    entry.setTableName(table);
    entry.setIndexName(idx);
    entry.setIndexUnique(false);
    entry.setIndexColumns(cols);
    entry.setStatus(status);
    return entry;
  }
}
