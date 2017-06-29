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

package org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;

/***/
@Sequence(300100)
@UUID("888828f0-2578-11e0-ac64-0800200c9a66")
public final class AddRating extends UpgradeStepTest {

  /**
   * @see UpgradeStep#execute(SchemaEditor, DataEditor)
   */
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addColumn("Cake", column("rating", DataType.DECIMAL).nullable(), new FieldLiteral(0d));
  }
}