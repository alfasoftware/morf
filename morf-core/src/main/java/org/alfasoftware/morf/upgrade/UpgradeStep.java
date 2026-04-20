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
   * <p><b>Override one of the two {@code execute} overloads — not both.</b>
   * Most steps override this 2-arg form and never see an {@link UpgradeContext}.
   * Steps that need upgrade-time context (e.g. the source schema) should
   * override
   * {@link #execute(SchemaEditor, DataEditor, UpgradeContext)} instead and
   * leave this 2-arg form with a throwing stub: the framework always calls
   * the 3-arg form, so this 2-arg body will never execute when its 3-arg
   * sibling is overridden.</p>
   *
   * @param schema {@link SchemaEditor} available for changing the database schema.
   * @param data {@link DataEditor} available for changing the database data.
   */
  public void execute(SchemaEditor schema, DataEditor data);


  /**
   * Context-aware variant of {@link #execute(SchemaEditor, DataEditor)}.
   *
   * <p><b>Override one of the two {@code execute} overloads — not both.</b>
   * The framework always invokes this 3-arg form; its default
   * implementation bridges to the 2-arg form for steps that don't need
   * context. Steps that need read access to pre-upgrade state
   * (e.g. the source schema via {@link UpgradeContext#getSourceSchema()})
   * should override this method and leave the 2-arg form with a throwing
   * stub.</p>
   *
   * <p>Overriding both is legal but not useful: the 2-arg body is dormant
   * (framework calls 3-arg, adopter's 3-arg runs; the 2-arg default bridge
   * is replaced and never reaches the 2-arg body). To combine the two, the
   * 3-arg override must invoke {@code execute(schema, data)} explicitly.</p>
   *
   * @param schema {@link SchemaEditor} available for changing the database schema.
   * @param data {@link DataEditor} available for changing the database data.
   * @param context read-only upgrade-time context (e.g. source schema).
   */
  public default void execute(SchemaEditor schema, DataEditor data, UpgradeContext context) {
    execute(schema, data);
  }
}
