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

package org.alfasoftware.morf.upgrade.upgrade;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.ExclusiveExecution;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;

/**
 * Create the {@code DeferredIndexOperation} table, which is used to track
 * index operations deferred for background execution.
 *
 * <p>{@link ExclusiveExecution} and {@code @Sequence(1)} ensure this step
 * runs before any step that uses {@code addIndexDeferred()}, which generates
 * INSERT statements targeting these tables. Without this guarantee,
 * {@link org.alfasoftware.morf.upgrade.GraphBasedUpgrade} could schedule
 * such steps in parallel, causing INSERTs to fail on a non-existent table.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ExclusiveExecution
@Sequence(1)
@UUID("4aa4bb56-74c4-4fb6-b896-84064f6d6fe3")
@Version("2.29.1")
public class CreateDeferredIndexOperationTables implements UpgradeStep {

  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getJiraId()
   */
  @Override
  public String getJiraId() {
    return "MORF-111";
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getDescription()
   */
  @Override
  public String getDescription() {
    return "Create tables for tracking deferred index operations";
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#execute(org.alfasoftware.morf.upgrade.SchemaEditor, org.alfasoftware.morf.upgrade.DataEditor)
   */
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addTable(
      table("DeferredIndexOperation")
        .columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("upgradeUUID", DataType.STRING, 100),
          column("tableName", DataType.STRING, 60),
          column("indexName", DataType.STRING, 60),
          column("indexUnique", DataType.BOOLEAN),
          column("indexColumns", DataType.STRING, 2000),
          column("status", DataType.STRING, 20),
          column("retryCount", DataType.INTEGER),
          column("createdTime", DataType.DECIMAL, 14),
          column("startedTime", DataType.DECIMAL, 14).nullable(),
          column("completedTime", DataType.DECIMAL, 14).nullable(),
          column("errorMessage", DataType.CLOB).nullable()
        )
        .indexes(
          index("DeferredIndexOp_1").columns("status"),
          index("DeferredIndexOp_2").columns("tableName")
        )
    );
  }
}
