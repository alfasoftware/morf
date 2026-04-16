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
 * Creates the DeployedIndexes table which tracks all deployed indexes
 * (deferred and non-deferred). Prepopulation with existing indexes is
 * handled by {@code Upgrade.findPath()} after this step runs.
 *
 * <p>Must run before any step that uses deferred indexes. The
 * {@link ExclusiveExecution} annotation ensures this runs alone,
 * not in parallel with other steps.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ExclusiveExecution
@Sequence(2)
@UUID("c7d8e9f0-1a2b-3c4d-5e6f-7a8b9c0d1e2f")
@Version("2.31.1")
public class CreateDeployedIndexes implements UpgradeStep {

  @Override
  public String getJiraId() {
    return "MORF-222";
  }

  @Override
  public String getDescription() {
    return "Create DeployedIndexes table for tracking all deployed indexes";
  }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addTable(
        table("DeployedIndexes")
            .columns(
                column("id", DataType.BIG_INTEGER).primaryKey(),
                column("upgradeUUID", DataType.STRING, 100).nullable(),
                column("tableName", DataType.STRING, 60),
                column("indexName", DataType.STRING, 60),
                column("indexUnique", DataType.BOOLEAN),
                column("indexColumns", DataType.STRING, 2000),
                column("indexDeferred", DataType.BOOLEAN),
                column("status", DataType.STRING, 20),
                column("retryCount", DataType.INTEGER),
                column("createdTime", DataType.DECIMAL, 14),
                column("startedTime", DataType.DECIMAL, 14).nullable(),
                column("completedTime", DataType.DECIMAL, 14).nullable(),
                column("errorMessage", DataType.CLOB).nullable()
            )
            .indexes(
                index("DeployedIdx_1").columns("tableName", "indexName").unique(),
                index("DeployedIdx_2").columns("status")
            )
    );
  }
}
