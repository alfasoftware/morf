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

package org.alfasoftware.morf.upgrade.db.v5_1_20;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Removes ranDate from UpgradeAudit table and adds appliedDate instead. ranDate is a String and
 * is not sortable by date. appliedDate is a bigint containing the date and time information and is sortable.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
@Sequence(1296040481)
@UUID("99ccdfc2-0553-49bb-8a1e-cc960bc32bad")
class RefactorRunDateToAppliedTimeInUpgradeAudit implements UpgradeStep {

  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getJiraId()
   */
  @Override
  public String getJiraId() {
    return "WEB-15700";
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getDescription()
   */
  @Override
  public String getDescription() {
    return "Substitute the ranDate string for a sortable appliedDate bigint field";
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#execute(org.alfasoftware.morf.upgrade.SchemaEditor, org.alfasoftware.morf.upgrade.DataEditor)
   */
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.removeColumn("UpgradeAudit", column("ranDate", DataType.STRING, 100).nullable());
    schema.addColumn("UpgradeAudit", column("appliedTime", DataType.DECIMAL, 14).nullable(), new FieldLiteral("20091001000000", DataType.DECIMAL));
  }

}
