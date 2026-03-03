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

package org.alfasoftware.morf.upgrade.deferred;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexRecoveryService} verifying stale
 * operation recovery with mocked DAO and schema dependencies.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexRecoveryServiceUnit {

  /** recoverStaleOperations should return immediately when no stale operations exist. */
  @Test
  public void testRecoverNoStaleOperations() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findStaleInProgressOperations(anyLong())).thenReturn(Collections.emptyList());

    DeferredIndexConfig config = new DeferredIndexConfig();
    ConnectionResources mockConn = mock(ConnectionResources.class);
    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(mockDao, mockConn, config);
    service.recoverStaleOperations();

    verify(mockDao).findStaleInProgressOperations(anyLong());
    verify(mockConn, never()).openSchemaResource();
  }


  /** A stale operation where the index already exists should be marked COMPLETED. */
  @Test
  public void testRecoverStaleOperationIndexExists() {
    DeferredIndexOperation op = buildOp(1L, "Product", "Product_Name_1");
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findStaleInProgressOperations(anyLong())).thenReturn(List.of(op));

    Schema schema = SchemaUtils.schema(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name")
        )
    );
    SchemaResource mockSchemaResource = mockSchemaResource(schema);
    ConnectionResources mockConn = mock(ConnectionResources.class);
    when(mockConn.openSchemaResource()).thenReturn(mockSchemaResource);

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(mockDao, mockConn, config);
    service.recoverStaleOperations();

    verify(mockDao).markCompleted(eq(1L), anyLong());
    verify(mockDao, never()).resetToPending(1L);
  }


  /** A stale operation where the index is absent should be reset to PENDING. */
  @Test
  public void testRecoverStaleOperationIndexAbsent() {
    DeferredIndexOperation op = buildOp(1L, "Product", "Product_Name_1");
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findStaleInProgressOperations(anyLong())).thenReturn(List.of(op));

    Schema schema = SchemaUtils.schema(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        )
    );
    SchemaResource mockSchemaResource = mockSchemaResource(schema);
    ConnectionResources mockConn = mock(ConnectionResources.class);
    when(mockConn.openSchemaResource()).thenReturn(mockSchemaResource);

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(mockDao, mockConn, config);
    service.recoverStaleOperations();

    verify(mockDao).resetToPending(1L);
    verify(mockDao, never()).markCompleted(eq(1L), anyLong());
  }


  /** A stale operation where the table does not exist should be reset to PENDING. */
  @Test
  public void testRecoverStaleOperationTableNotFound() {
    DeferredIndexOperation op = buildOp(1L, "NonExistentTable", "NonExistentTable_1");
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findStaleInProgressOperations(anyLong())).thenReturn(List.of(op));

    Schema schema = SchemaUtils.schema(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey()
        )
    );
    SchemaResource mockSchemaResource = mockSchemaResource(schema);
    ConnectionResources mockConn = mock(ConnectionResources.class);
    when(mockConn.openSchemaResource()).thenReturn(mockSchemaResource);

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(mockDao, mockConn, config);
    service.recoverStaleOperations();

    verify(mockDao).resetToPending(1L);
    verify(mockDao, never()).markCompleted(eq(1L), anyLong());
  }


  /** Multiple stale operations should each be recovered independently. */
  @Test
  public void testRecoverMultipleStaleOperations() {
    DeferredIndexOperation opExists = buildOp(1L, "Product", "Product_Name_1");
    DeferredIndexOperation opAbsent = buildOp(2L, "Product", "Product_Code_1");
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findStaleInProgressOperations(anyLong())).thenReturn(List.of(opExists, opAbsent));

    Schema schema = SchemaUtils.schema(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100),
            column("code", DataType.STRING, 20)
        ).indexes(
            index("Product_Name_1").columns("name")
        )
    );
    SchemaResource mockSchemaResource = mockSchemaResource(schema);
    ConnectionResources mockConn = mock(ConnectionResources.class);
    when(mockConn.openSchemaResource()).thenReturn(mockSchemaResource);

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(mockDao, mockConn, config);
    service.recoverStaleOperations();

    verify(mockDao).markCompleted(eq(1L), anyLong());
    verify(mockDao).resetToPending(2L);
  }


  /** Index name comparison should be case-insensitive (e.g. H2 folds to uppercase). */
  @Test
  public void testRecoverIndexExistsCaseInsensitive() {
    DeferredIndexOperation op = buildOp(1L, "Product", "product_name_1");
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findStaleInProgressOperations(anyLong())).thenReturn(List.of(op));

    Schema schema = SchemaUtils.schema(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("PRODUCT_NAME_1").columns("name")
        )
    );
    SchemaResource mockSchemaResource = mockSchemaResource(schema);
    ConnectionResources mockConn = mock(ConnectionResources.class);
    when(mockConn.openSchemaResource()).thenReturn(mockSchemaResource);

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(mockDao, mockConn, config);
    service.recoverStaleOperations();

    verify(mockDao).markCompleted(eq(1L), anyLong());
    verify(mockDao, never()).resetToPending(1L);
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private DeferredIndexOperation buildOp(long id, String tableName, String indexName) {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setId(id);
    op.setUpgradeUUID("test-uuid");
    op.setTableName(tableName);
    op.setIndexName(indexName);
    op.setOperationType(DeferredIndexOperationType.ADD);
    op.setIndexUnique(false);
    op.setStatus(DeferredIndexStatus.IN_PROGRESS);
    op.setRetryCount(0);
    op.setCreatedTime(20260101120000L);
    op.setStartedTime(20260101110000L);
    op.setColumnNames(List.of("col1"));
    return op;
  }


  private SchemaResource mockSchemaResource(Schema schema) {
    return new SchemaResource() {
      @Override
      public boolean tableExists(String name) {
        return schema.tableExists(name);
      }

      @Override
      public org.alfasoftware.morf.metadata.Table getTable(String name) {
        return schema.getTable(name);
      }

      @Override
      public java.util.Collection<String> tableNames() {
        return schema.tableNames();
      }

      @Override
      public java.util.Collection<org.alfasoftware.morf.metadata.Table> tables() {
        return schema.tables();
      }

      @Override
      public boolean viewExists(String name) {
        return schema.viewExists(name);
      }

      @Override
      public org.alfasoftware.morf.metadata.View getView(String name) {
        return schema.getView(name);
      }

      @Override
      public java.util.Collection<String> viewNames() {
        return schema.viewNames();
      }

      @Override
      public java.util.Collection<org.alfasoftware.morf.metadata.View> views() {
        return schema.views();
      }

      @Override
      public boolean sequenceExists(String name) {
        return schema.sequenceExists(name);
      }

      @Override
      public org.alfasoftware.morf.metadata.Sequence getSequence(String name) {
        return schema.getSequence(name);
      }

      @Override
      public java.util.Collection<String> sequenceNames() {
        return schema.sequenceNames();
      }

      @Override
      public java.util.Collection<org.alfasoftware.morf.metadata.Sequence> sequences() {
        return schema.sequences();
      }

      @Override
      public boolean isEmptyDatabase() {
        return schema.isEmptyDatabase();
      }

      @Override
      public void close() {
        // No-op for testing
      }
    };
  }
}
