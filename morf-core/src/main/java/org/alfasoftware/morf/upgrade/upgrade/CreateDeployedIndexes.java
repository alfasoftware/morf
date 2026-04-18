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
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;

import java.util.UUID;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.ExclusiveExecution;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

/**
 * Creates the DeployedIndexes table and prepopulates it with all existing
 * indexes from the source schema.
 *
 * <p>Must run before any step that uses deferred indexes. The
 * {@link ExclusiveExecution} annotation ensures this runs alone,
 * not in parallel with other steps.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ExclusiveExecution
@Sequence(2)
@org.alfasoftware.morf.upgrade.UUID("c7d8e9f0-1a2b-3c4d-5e6f-7a8b9c0d1e2f")
@Version("2.31.1")
public class CreateDeployedIndexes implements UpgradeStep {

  private static final String DEPLOYED_INDEXES = DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME;

  @Override
  public String getJiraId() {
    return "MORF-222";
  }

  @Override
  public String getDescription() {
    return "Create DeployedIndexes table and prepopulate with existing indexes";
  }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    // Create the table
    schema.addTable(
        table(DEPLOYED_INDEXES)
            .columns(
                column("id", DataType.BIG_INTEGER).primaryKey(),
                column("tableName", DataType.STRING, 60),
                column("indexName", DataType.STRING, 60),
                column("indexUnique", DataType.BOOLEAN),
                column("indexColumns", DataType.STRING, 4000),
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

    // Prepopulate with all existing indexes from the source schema
    Schema sourceSchema = schema.getSourceSchema();
    long createdTime = System.currentTimeMillis();

    for (Table sourceTable : sourceSchema.tables()) {
      for (Index idx : sourceTable.indexes()) {
        if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(idx.getName())) {
          continue;
        }
        long id = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        data.executeStatement(
            insert().into(tableRef(DEPLOYED_INDEXES))
                .values(
                    literal(id).as("id"),
                    literal(sourceTable.getName()).as("tableName"),
                    literal(idx.getName()).as("indexName"),
                    literal(idx.isUnique()).as("indexUnique"),
                    literal(String.join(",", idx.columnNames())).as("indexColumns"),
                    literal(false).as("indexDeferred"),
                    literal("COMPLETED").as("status"),
                    literal(0).as("retryCount"),
                    literal(createdTime).as("createdTime")
                )
        );
      }
    }
  }
}
