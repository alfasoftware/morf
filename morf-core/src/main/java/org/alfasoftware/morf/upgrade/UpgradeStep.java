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
 * Defines a database upgrade that may comprise both schema and data changes to
 * provide a new feature.
 *
 * <p>Implementations must support no argument constructors.</p>
 *
 * <p>The following annotations must used on implementations of this interface:</p>
 * <dl>
 * <dt>{@link UUID}</dt><dd>A unique identifier for this upgrade, which must never
 * change. Generate using {@link java.util.UUID#randomUUID()}</dd>
 * <dt>{@link Sequence}</dt><dd>The sequence number for the upgrade. Implies ordering
 * but does not have to be unique. For most organisations, the number of seconds
 * since the epoch works well. This is sufficient in most cases to ensure that
 * mutually dependent upgrades are run in their dependency order.</dd>
 * </dl>
 *
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface UpgradeStep {

  /**
   * The JIRA reference for the upgrade step. This should generally refer to the
   * WEB issue under which this the database development was performed,
   * <strong>but must be an issue which will appear in the release notes</strong>.
   *
   * @return a JIRA ID.
   */
  public String getJiraId();


  /**
   * The human readable, English, description of this upgrade step. This should
   * be one sentence which encapsulates the purpose of the change. This shouldn't
   * have a full stop, as it will appear in a listing.
   *
   * <p><b>For example:</b> 'Add support for internationalised invoice messages'</p>
   *
   * <p><b>Not:</b> 'Add column messageKeyId to InvoiceMessage.'</p>
   *
   * @return A single English sentence.
   */
  public String getDescription();


  /**
   * Implemented by upgrade authors to specify the sequence of changes required
   * to bring a database to the required state.
   *
   * @param schema {@link SchemaEditor} available for changing the database schema.
   * @param data {@link DataEditor} available for changing the database data.
   */
  public void execute(SchemaEditor schema, DataEditor data);
}
