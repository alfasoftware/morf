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

/**
 * Describes the status of the upgrade: this is stored in a transient table,
 * managed by {@link UpgradeStatusTableService}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public enum UpgradeStatus {

  /**
   * There is no upgrade in progress.
   */
  NONE,

  /**
   * The schema upgrade has completed but a data transfer is required before the application can start.
   */
  DATA_TRANSFER_REQUIRED,

  /**
   * A data transfer is in progress.
   */
  DATA_TRANSFER_IN_PROGRESS,

  /**
   * Upgrade is in progress.
   */
  IN_PROGRESS,

  /**
   * Upgrade failed.
   */
  FAILED,

  /**
   * Upgrade has been completed.
   */
  COMPLETED;
}
