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
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

/**
 * Creates the DeferredIndexes registration table.
 *
 * <p>The table only ever holds rows for deferred indexes, so there's no
 * prepopulation step — nothing to seed for indexes that existed before the
 * feature was introduced (they're non-deferred and live only in the physical
 * DB, where {@code SchemaHomology} handles them).</p>
 *
 * <p>Runs under {@link ExclusiveExecution} so it can't race with other
 * steps.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ExclusiveExecution
@Sequence(1)
@org.alfasoftware.morf.upgrade.UUID("c7d8e9f0-1a2b-3c4d-5e6f-7a8b9c0d1e2f")
@Version("2.31.1")
public class CreateDeferredIndexes implements UpgradeStep {

  private static final String DEFERRED_INDEXES = DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME;

  @Override
  public String getJiraId() {
    return "MORF-225";
  }

  @Override
  public String getDescription() {
    return "Create DeferredIndexes table";
  }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addTable(
        table(DEFERRED_INDEXES)
            .columns(
                column("id", DataType.BIG_INTEGER).primaryKey(),
                column("tableName", DataType.STRING, 60),
                column("indexName", DataType.STRING, 60),
                column("indexUnique", DataType.BOOLEAN),
                column("indexColumns", DataType.STRING, 4000),
                column("status", DataType.STRING, 20),
                column("attemptsCount", DataType.INTEGER),
                column("createdTime", DataType.DECIMAL, 14),
                column("startedTime", DataType.DECIMAL, 14).nullable(),
                column("completedTime", DataType.DECIMAL, 14).nullable(),
                column("errorMessage", DataType.CLOB).nullable()
            )
            .indexes(
                index("DeferredIdx_1").columns("tableName", "indexName").unique(),
                index("DeferredIdx_2").columns("status")
            )
    );
  }
}
