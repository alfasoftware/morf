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

package org.alfasoftware.morf.upgrade.deployed;

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
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
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


  /** When feature is disabled, enrichSchema returns input unchanged. */
  @Test
  public void testDisabledReturnsInputUnchanged() {
    // given
    config.setDeferredIndexCreationEnabled(false);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Schema result = enricher.enrichSchema(input);

    // then
    assertSame(input, result);
  }


  /** When DeployedIndexes table doesn't exist, returns input unchanged. */
  @Test
  public void testNoDeployedIndexesTableReturnsUnchanged() {
    // given -- schema without DeployedIndexes table
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Schema result = enricher.enrichSchema(input);

    // then
    assertSame(input, result);
  }


  /** When DeployedIndexes table is empty, returns input unchanged. */
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
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Schema result = enricher.enrichSchema(input);

    // then
    assertSame(input, result);
  }


  /** Physical index with matching DeployedIndexes row should be enriched. */
  @Test
  public void testPhysicalIndexEnrichedWithDeployedData() {
    // given
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("MyIdx").columns("id"))
    );
    DeployedIndexEntry entry = new DeployedIndexEntry();
    entry.setTableName("MyTable");
    entry.setIndexName("MyIdx");
    entry.setIndexDeferred(true);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Schema result = enricher.enrichSchema(input);

    // then
    Index enrichedIdx = result.getTable("MyTable").indexes().get(0);
    assertTrue("Should be deferred", enrichedIdx.isDeferred());
    assertTrue("Should be physically present", enrichedIdx.isPhysicallyPresent());
  }


  /** Deferred index with no physical counterpart should be added as virtual. */
  @Test
  public void testDeferredIndexAddedAsVirtual() {
    // given -- table with no physical indexes
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey(), column("name", DataType.STRING, 50))
    );
    DeployedIndexEntry entry = new DeployedIndexEntry();
    entry.setTableName("MyTable");
    entry.setIndexName("MyIdx");
    entry.setIndexDeferred(true);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("name"));
    entry.setStatus(DeployedIndexStatus.PENDING);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when
    Schema result = enricher.enrichSchema(input);

    // then
    assertEquals(1, result.getTable("MyTable").indexes().size());
    Index virtual = result.getTable("MyTable").indexes().get(0);
    assertEquals("MyIdx", virtual.getName());
    assertTrue("Should be deferred", virtual.isDeferred());
    assertFalse("Should not be physically present", virtual.isPhysicallyPresent());
  }


  /** Non-deferred index missing from DB should throw error. */
  @Test(expected = IllegalStateException.class)
  public void testNonDeferredMissingFromDbThrowsError() {
    // given -- DeployedIndexes says non-deferred index exists, but it's not in the physical schema
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
    );
    DeployedIndexEntry entry = new DeployedIndexEntry();
    entry.setTableName("MyTable");
    entry.setIndexName("MissingIdx");
    entry.setIndexDeferred(false);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when -- should throw
    enricher.enrichSchema(input);
  }


  /** Physical index not tracked in DeployedIndexes should throw error. */
  @Test(expected = IllegalStateException.class)
  public void testUntrackedPhysicalIndexThrowsError() {
    // given -- physical index exists but no DeployedIndexes row
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("UntrackedIdx").columns("id"))
    );
    // DAO returns entry for a DIFFERENT index
    DeployedIndexEntry entry = new DeployedIndexEntry();
    entry.setTableName("MyTable");
    entry.setIndexName("OtherIdx");
    entry.setIndexDeferred(false);
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("id"));
    entry.setStatus(DeployedIndexStatus.COMPLETED);
    when(dao.findAll()).thenReturn(List.of(entry));
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when -- should throw
    enricher.enrichSchema(input);
  }


  /** _PRF indexes should be excluded from validation. */
  @Test
  public void testPrfIndexExcludedFromValidation() {
    // given -- _PRF index in physical schema with no DeployedIndexes row
    Schema input = schema(
        table(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)
            .columns(column("id", DataType.BIG_INTEGER).primaryKey()),
        table("MyTable").columns(column("id", DataType.BIG_INTEGER).primaryKey())
            .indexes(index("MyTable_PRF1").columns("id"))
    );
    when(dao.findAll()).thenReturn(Collections.emptyList());
    DeployedIndexesModelEnricher enricher = new DeployedIndexesModelEnricher(dao, config);

    // when -- should NOT throw despite untracked PRF index
    Schema result = enricher.enrichSchema(input);

    // then
    assertTrue("PRF index should pass through", result.getTable("MyTable").indexes().stream()
        .anyMatch(i -> "MyTable_PRF1".equals(i.getName())));
  }
}
