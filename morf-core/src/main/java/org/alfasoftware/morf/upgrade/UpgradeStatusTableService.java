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

package org.alfasoftware.morf.upgrade;

import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import com.google.inject.ImplementedBy;

/**
 * Service to manage or generate SQL for a transient table that stores the upgrade status.
 *
 * <p>The transient table, <code>{@value UpgradeStatusTableService#UPGRADE_STATUS}</code>, is
 * created at the beginning of an upgrade. The table will first contain the status
 * {@code UpgradeStatus#IN_PROGRESS} then will be updated in accordance with the different steps
 * that the upgrade is going through - from {@code UpgradeStatus#DATA_TRANSFER_REQUIRED} to
 * {@code UpgradeStatus#COMPLETED}.</p>
 *
 * <p>Once the upgrade is {@code UpgradeStatus#COMPLETED}, the table is deleted and the
 * application can start.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@ImplementedBy(UpgradeStatusTableServiceImpl.class)
public interface UpgradeStatusTableService {

  /**
   * Name of the transient table that will be used to store the current
   * state of the database upgrade. The value in this table will be one of
   * the values from {@link UpgradeStatus}.
   */
  String UPGRADE_STATUS = "zzzUpgradeStatus";


  /**
   * Change the status of the upgrade, recording it in the temporary table.
   * This is performed atomically, and verifies the current status in the
   * table.
   *
   * @param fromStatus the status that must be the current status for the write
   *          to be performed. If the current status in the transient table is not
   *          {@code fromStatus}, this method will not do anything.
   * @param toStatus the new status.
   * @return the number of rows updated. Will return 0 if the current status does not match;
   *          and will be 1 if the update of the status completed successfully.
   */
  int writeStatusFromStatus(UpgradeStatus fromStatus, UpgradeStatus toStatus);


  /**
   * Generate the script needed to update the transient
   * <code>{@value UpgradeStatusTableService#UPGRADE_STATUS}</code> table
   * for the required SQL platform. This may involve creating the
   * table, depending on {@code fromStatus} and {@code toStatus}.
   *
   * @param fromStatus the status that must be the current status for the write
   *          to be performed. If the current status in the transient table is not
   *          {@code fromStatus}, this method will not do anything.
   * @param toStatus the new status.
   * @return the SQL to update {@value UpgradeStatusTableService#UPGRADE_STATUS} as appropriate.
   */
  List<String> updateTableScript(UpgradeStatus fromStatus, UpgradeStatus toStatus);


  /**
   * Gets the current upgrade status. The non-existence of the transient table
   * corresponds to {@link UpgradeStatus#NONE}.
   *
   * @param dataSource the dataSource to use to execute the select statement which retrieve the status.
   * @return the current status.
   */
  UpgradeStatus getStatus(Optional<DataSource> dataSource);


  /**
   * Tidy up the upgrade status table. This marks the upgrade procedure as complete, and further
   * calls to {@link #getStatus(Optional)} will return {@code NONE}. As higher privileged connections
   * may be needed to delete the <code>{@value UpgradeStatusTableService#UPGRADE_STATUS}</code> table,
   * a data source must be provided.
   *
   * @param dataSource Data source to use.
   */
  void tidyUp(DataSource dataSource);
}

