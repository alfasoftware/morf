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

package org.alfasoftware.morf.upgrade.deferredindexes;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexesModelEnricher} (background-build model).
 *
 * <p>Drift policy is narrow: only operator-caused corruption of COMPLETED
 * rows throws. Non-COMPLETED rows with a present physical index — the
 * routine-restart case — are rebuilt as deferred and the build task
 * reconciles them on the next pass.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexesModelEnricherImpl {

  private DeferredIndexesDAO dao;
  private DeferredIndexSession session;
  private UpgradeConfigAndContext config;
  private ConnectionResources connectionResources;
  private SqlDialect dialect;
  private Connection connection;


  @Before
  public void setUp() throws Exception {
    dao = mock(DeferredIndexesDAO.class);
    session = new DeferredIndexSessionImpl(new DeferredIndexesStatements());
    config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);

    connectionResources = mock(ConnectionResources.class);
    dialect = mock(SqlDialect.class);
    connection = mock(Connection.class);
    DataSource dataSource = mock(DataSource.class);
    when(connectionResources.sqlDialect()).thenReturn(dialect);
    when(connectionResources.getDataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
    // Default: dialects without an isIndexValid implementation return empty;
    // the enricher's orElse(true) treats this as VALID.
    when(dialect.isIndexValid(any(), anyString(), anyString())).thenReturn(Optional.empty());
  }


  /** When feature is disabled, enrich returns input schema unchanged. */
  @Test
  public void testDisabledReturnsInputUnchanged() {
    // given
    config.setDeferredIndexCreationEnabled(false);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when
    Schema result = enricher.enrich(input, session);

    // then
    assertSame(input, result);
  }


  /** When DeferredIndexes table doesn't exist, returns input schema unchanged. */
  @Test
  public void testNoDeployedIndexesTableReturnsUnchanged() {
    // given
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when
    Schema result = enricher.enrich(input, session);

    // then
    assertSame(input, result);
  }


  /** When DeferredIndexes table is empty, returns input schema unchanged. */
  @Test
  public void testEmptyDeployedIndexesReturnsUnchanged() {
    // given
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("Foo_1").columns("id"))
    );
    when(dao.findAll()).thenReturn(Collections.emptyList());
    DeferredIndexesModelEnricher enricher = newEnricher();

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
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
    );
    DeferredIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeferredIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeferredIndexesModelEnricher enricher = newEnricher();

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


  /**
   * Non-COMPLETED row + matching physical → rebuilt as deferred (USED to
   * throw on the slim branch). This is the routine-restart case: the build
   * task crashed mid-build; the next pass will see {@code isIndexValid} and
   * either mark COMPLETED or DROP+CREATE.
   */
  @Test
  public void testNonCompletedRowWithPhysicalMatchRebuiltAsDeferred() {
    // given — physical index exists but tracking row says PENDING
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
            .indexes(index("MyIdx").columns("name"))
    );
    DeferredIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeferredIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when — does NOT throw
    Schema result = enricher.enrich(input, session);

    // then — physical rebuilt with .deferred() flag
    Index enriched = result.getTable("MyTable").indexes().get(0);
    assertEquals("MyIdx", enriched.getName());
    assertTrue("Non-COMPLETED + physical present should be marked deferred for the build task",
        enriched.isDeferred());
    // and — session knows it's tracked AND awaiting build (status=PENDING)
    assertTrue(session.isTrackedDeferred("MyTable", "MyIdx"));
    assertTrue("Non-COMPLETED row should still be awaiting build",
        session.isAwaitingBuild("MyTable", "MyIdx"));
  }


  /** Same case for IN_PROGRESS — also no throw, build task self-heals. */
  @Test
  public void testInProgressRowWithPhysicalMatchRebuiltAsDeferred() {
    // given
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
            .indexes(index("MyIdx").columns("name"))
    );
    DeferredIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeferredIndexStatus.IN_PROGRESS);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when / then — no throw
    Schema result = enricher.enrich(input, session);
    assertTrue(result.getTable("MyTable").indexes().get(0).isDeferred());
  }


  /**
   * COMPLETED row + matching physical + dialect reports VALID → rebuilt with
   * {@code .deferred()} flag; session sees it as NOT awaiting (built).
   */
  @Test
  public void testCompletedDeferredWithValidPhysicalRebuiltAsDeferred() {
    // given — physical index exists, tracking row says COMPLETED, dialect reports VALID
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
            .indexes(index("MyIdx").columns("name"))
    );
    DeferredIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeferredIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    when(dialect.isIndexValid(eq(connection), eq("MyTable"), eq("MyIdx")))
        .thenReturn(Optional.of(Boolean.TRUE));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when
    Schema result = enricher.enrich(input, session);

    // then
    Index enriched = result.getTable("MyTable").indexes().get(0);
    assertTrue(enriched.isDeferred());
    assertTrue(session.isTrackedDeferred("MyTable", "MyIdx"));
    assertFalse("Built deferred should NOT be awaiting build",
        session.isAwaitingBuild("MyTable", "MyIdx"));
  }


  /**
   * COMPLETED row + matching physical + dialect returns empty (unknown — e.g.
   * MySQL or SQL Server) → rebuilt with {@code .deferred()} (orElse(true)).
   */
  @Test
  public void testCompletedDeferredWithUnknownValidityTreatedAsValid() {
    // given
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
            .indexes(index("MyIdx").columns("name"))
    );
    DeferredIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeferredIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    when(dialect.isIndexValid(eq(connection), eq("MyTable"), eq("MyIdx")))
        .thenReturn(Optional.empty());
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when / then — does not throw, rebuilds as deferred
    Schema result = enricher.enrich(input, session);
    assertTrue(result.getTable("MyTable").indexes().get(0).isDeferred());
  }


  /**
   * COMPLETED row + matching physical + dialect reports INVALID → drift,
   * throws. The executor cannot auto-recover; operator must intervene.
   */
  @Test
  public void testCompletedRowWithInvalidPhysicalThrowsDrift() {
    // given
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
            .indexes(index("MyIdx").columns("name"))
    );
    DeferredIndex entry = makeRow("MyTable", "MyIdx", List.of("name"), DeferredIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    when(dialect.isIndexValid(eq(connection), eq("MyTable"), eq("MyIdx")))
        .thenReturn(Optional.of(Boolean.FALSE));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when / then
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> enricher.enrich(input, session));
    assertTrue("Message should mention the index",
        ex.getMessage().contains("MyIdx"));
    assertTrue("Message should mention COMPLETED",
        ex.getMessage().contains("COMPLETED"));
    assertTrue("Message should mention INVALID",
        ex.getMessage().contains("INVALID"));
    assertTrue("Message should hint at manual recovery",
        ex.getMessage().toLowerCase().contains("manually"));
  }


  /** COMPLETED row + NO physical match → drift; sharpened message hints at manual recovery. */
  @Test
  public void testCompletedRowWithoutPhysicalMatchThrowsDrift() {
    // given — tracking row says COMPLETED but physical index is missing
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            // no physical MyIdx
    );
    DeferredIndex entry = makeRow("MyTable", "MyIdx", List.of("id"), DeferredIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when / then
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> enricher.enrich(input, session));
    assertTrue("Message should mention the missing index",
        ex.getMessage().contains("MyIdx"));
    assertTrue("Message should mention COMPLETED",
        ex.getMessage().contains("COMPLETED"));
    assertTrue("Message should mention manual recovery",
        ex.getMessage().toLowerCase().contains("backup")
        || ex.getMessage().toLowerCase().contains("manual"));
  }


  /** Row references a table not in the physical schema → throws IllegalStateException. */
  @Test
  public void testRowReferencingMissingTableThrowsDrift() {
    // given
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey())
            // no other tables
    );
    DeferredIndex entry = makeRow("Ghost", "GhostIdx", List.of("id"), DeferredIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when / then
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> enricher.enrich(input, session));
    assertTrue("Message should mention the orphan table",
        ex.getMessage().contains("Ghost"));
  }


  /**
   * Multi-drift collection: when several COMPLETED-row anomalies exist
   * across the schema (missing physical, INVALID physical, orphan table),
   * every single one is reported in the single {@link IllegalStateException}
   * — operator sees the full picture in one boot cycle rather than fixing
   * one issue, restarting, and finding the next.
   */
  @Test
  public void testCollectsMultipleDriftsInOneException() {
    // given — three independent drift sources:
    //   (1) COMPLETED row whose physical is present but INVALID
    //   (2) COMPLETED row whose physical is missing
    //   (3) tracking row referencing a table not in the physical schema
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("Alpha").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
            .indexes(index("Alpha_Idx").columns("name")),
        table("Beta").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            // no physical Beta_Idx
    );
    DeferredIndex invalidPhys = makeRow("Alpha", "Alpha_Idx", List.of("name"), DeferredIndexStatus.COMPLETED);
    DeferredIndex missingPhys = makeRow("Beta",  "Beta_Idx",  List.of("id"),   DeferredIndexStatus.COMPLETED);
    DeferredIndex orphanTable = makeRow("Ghost", "GhostIdx",  List.of("id"),   DeferredIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(invalidPhys, missingPhys, orphanTable));
    when(dialect.isIndexValid(eq(connection), eq("Alpha"), eq("Alpha_Idx")))
        .thenReturn(Optional.of(Boolean.FALSE));
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when / then — single exception mentioning every distinct drift
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> enricher.enrich(input, session));
    assertTrue("Message should report a count of 3 drifts: " + ex.getMessage(),
        ex.getMessage().contains("3 issue"));
    assertTrue("Message should mention Alpha_Idx INVALID drift",
        ex.getMessage().contains("Alpha_Idx") && ex.getMessage().contains("INVALID"));
    assertTrue("Message should mention Beta_Idx missing-physical drift",
        ex.getMessage().contains("Beta_Idx") && ex.getMessage().contains("missing"));
    assertTrue("Message should mention orphan-table GhostIdx",
        ex.getMessage().contains("GhostIdx") && ex.getMessage().contains("Ghost"));
  }


  /** Enricher primes the session with every persisted row regardless of status. */
  @Test
  public void testEnrichPrimesSessionWithEveryPersistedRow() {
    // given — two persisted rows, one COMPLETED one PENDING
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("TableA").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("A_Idx").columns("id")),
        table("TableB").columns(column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50))
    );
    DeferredIndex entryA = makeRow("TableA", "A_Idx", List.of("id"), DeferredIndexStatus.COMPLETED);
    DeferredIndex entryB = makeRow("TableB", "B_Idx", List.of("name"), DeferredIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entryA, entryB));
    DeferredIndexSession mockSession = mock(DeferredIndexSession.class);
    DeferredIndexesModelEnricher enricher = newEnricher();

    // when
    enricher.enrich(input, mockSession);

    // then — both rows primed
    verify(mockSession).prime(entryA);
    verify(mockSession).prime(entryB);
  }


  // ---- helpers --------------------------------------------------------------

  private DeferredIndexesModelEnricher newEnricher() {
    return new DeferredIndexesModelEnricherImpl(dao, connectionResources, config);
  }


  private static DeferredIndex makeRow(String table, String idx, List<String> cols,
                                        DeferredIndexStatus status) {
    DeferredIndex entry = new DeferredIndex();
    entry.setTableName(table);
    entry.setIndexName(idx);
    entry.setIndexUnique(false);
    entry.setIndexColumns(cols);
    entry.setStatus(status);
    return entry;
  }
}
