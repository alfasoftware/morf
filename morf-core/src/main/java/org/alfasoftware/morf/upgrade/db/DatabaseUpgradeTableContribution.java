/* Copyright 2017 Alfa Financial Software
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
package org.alfasoftware.morf.upgrade.db;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.TableContribution;
import org.alfasoftware.morf.upgrade.UpgradeStep;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tables required by Morf to manage upgrades.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class DatabaseUpgradeTableContribution implements TableContribution {
  /** Name of the table containing information on the upgrade steps applied within the app's database. */
  public static final String UPGRADE_AUDIT_NAME = "UpgradeAudit";

  /** Name of the table containing information on the views deployed within the app's database. */
  public static final String DEPLOYED_VIEWS_NAME = "DeployedViews";


  /**
   * @return The Table descriptor of UpgradeAudit
   */
  public static Table upgradeAuditTable() {
    return table(UPGRADE_AUDIT_NAME)
        .columns(
          column("upgradeUUID", DataType.STRING, 100).primaryKey(),
          column("description", DataType.STRING, 200).nullable(),
          column("appliedTime", DataType.DECIMAL, 14).nullable()
        );
  }


  /**
   * @return The Table descriptor of DeployedViews
   */
  public static TableBuilder deployedViewsTable() {
    return table(DEPLOYED_VIEWS_NAME)
        .columns(
          column("name", DataType.STRING, 100).primaryKey(),
          column("hash", DataType.STRING, 64),
          column("sqlDefinition", DataType.CLOB).nullable()
        );
  }


  /**
   * @see org.alfasoftware.morf.upgrade.TableContribution#tables()
   */
  @Override
  public Collection<Table> tables() {
    return ImmutableList.of(
      deployedViewsTable(),
      upgradeAuditTable()
    );
  }


  /**
   * @see org.alfasoftware.morf.upgrade.TableContribution#schemaUpgradeClassses()
   */
  @Override
  public Collection<Class<? extends UpgradeStep>> schemaUpgradeClassses() {
    List<Class<? extends UpgradeStep>> result = Lists.newLinkedList();
    result.addAll(org.alfasoftware.morf.upgrade.upgrade.UpgradeSteps.LIST);
    return ImmutableList.copyOf(result);
  }
}
