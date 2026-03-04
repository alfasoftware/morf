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

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.ExclusiveExecution;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

/**
 * Create the {@code DeferredIndexOperation} and {@code DeferredIndexOperationColumn} tables,
 * which are used to track index operations deferred for background execution.
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
    return "MORF-1";
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
    schema.addTable(DatabaseUpgradeTableContribution.deferredIndexOperationTable());
    schema.addTable(DatabaseUpgradeTableContribution.deferredIndexOperationColumnTable());
  }
}
