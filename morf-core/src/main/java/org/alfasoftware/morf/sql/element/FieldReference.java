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

package org.alfasoftware.morf.sql.element;

import static org.alfasoftware.morf.util.DeepCopyTransformations.noTransformation;

import java.util.Optional;

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A field is used by a query to represent a column within the database. This class
 * has no concept of the type or representation of a field, only its name, alias and
 * the table on which it exists.
 *
 * <h3>Examples of use:</h3>
 *
 * <p>Create a field with a given name:</p>
 * <blockquote><pre>
 *    AliasedField newField = field("agreementnumber").build();</pre></blockquote>
 *
 * <p>Create a field with a given name and alias of "bob". This is equivalent to "agreementnumber AS bob" in SQL:</p>
 * <blockquote><pre>
 *    AliasedField newField = field("agreementnumber", "bob").build();</pre></blockquote>
 *
 * <p>Create a field which is sorted descending:</p>
 * <blockquote><pre>
 *    AliasedField newField = field("agreementnumber").asc().build();</pre></blockquote>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class FieldReference extends AliasedField implements Driver {

  /**
   * The table that the field relates to
   */
  private final TableReference table;

  /**
   * The field's name
   */
  private final String name;

  /**
   * The direction to use when this field is in an ORDER BY statement.
   * TODO this should be final as soon as we can remove {@link #setDirection(Direction)}
   * and {@link AliasedField#IMMUTABLE_BUILDERS_ENABLED}.
   */
  private Direction direction;

  /**
   * Handling of null values when executing ORDER BY statement.
   * TODO this vcan be made final when we remove {@link AliasedField#IMMUTABLE_BUILDERS_ENABLED}
   */
  private Optional<NullValueHandling> nullValueHandling;


  /**
   * Constructs a new field with an alias on a given table.
   *
   * @param name the name of the field.
   * @return The field reference.
   */
  public static Builder field(String name) {
    return new Builder(null, name);
  }


  /**
   * Constructs a new field with an alias on a given table.
   *
   * @param table the table on which the field exists
   * @param name the name of the field.
   * @return The field reference.
   */
  public static Builder field(TableReference table, String name) {
    return new Builder(table, name);
  }


  /**
   * Constructor used to create the deep copy of this field reference
   *
   * @param sourceField the field reference to create the copy from
   * @param transformer The transformation to be executed during the copy
   * @deprecated Use {@link #field(TableReference, String)}
   */
  @Deprecated
  protected FieldReference(FieldReference sourceField, DeepCopyTransformation transformer) {
    this(
        sourceField.getAlias(),
        transformer.deepCopy(sourceField.table),
        sourceField.name,
        sourceField.direction,
        sourceField.nullValueHandling
    );
  }


  /**
   * Constructs a new field with a specific sort direction on a given table.
   *
   * @param table the table on which the field exists
   * @param name the name of the field
   * @param direction the sort direction of the field
   * @param nullValueHandling how to handle nulls
   */
  public FieldReference(TableReference table, String name, Direction direction, NullValueHandling nullValueHandling) {
    this("", table, name, direction, Optional.of(nullValueHandling));
  }


  private FieldReference(String alias, TableReference table, String name, Direction direction, Optional<NullValueHandling> nullValueHandling) {
    super(alias);
    this.table = table;
    this.name = name;
    this.direction = direction;
    this.nullValueHandling = nullValueHandling;
  }


  /**
   * Constructs a new field with a specific sort direction on a given table.
   *
   * @param table the table on which the field exists
   * @param name the name of the field
   * @param direction the sort direction of the field
   */
  public FieldReference(TableReference table, String name, Direction direction) {
    this("", table, name, direction, Optional.<NullValueHandling>empty());
  }


  /**
   * Constructs a new field with an alias on a given table.
   *
   * @param table the table on which the field exists
   * @param name the name of the field
   */
  public FieldReference(TableReference table, String name) {
    this("", table, name, Direction.NONE, Optional.<NullValueHandling>empty());
  }


  /**
   * Constructs a new field with a given name
   *
   * @param name the name of the field
   */
  public FieldReference(String name) {
    this("", null, name, Direction.NONE, Optional.<NullValueHandling>empty());
  }


  /**
   * Constructs a new field with a name and a sort direction.
   *
   * @param name the name of the field
   * @param direction the sort direction for the field
   */
  public FieldReference(String name, Direction direction) {
    this("", null, name, direction, Optional.<NullValueHandling>empty());
  }


  /**
   * Gets the name of the field.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }


  /**
   * Gets the sort direction of the field.
   *
   * @return the direction
   */
  public Direction getDirection() {
    return direction;
  }


  /**
   * Gets the null value handling type of the field.
   *
   * @return the direction
   */
  public Optional<NullValueHandling> getNullValueHandling() {
    return nullValueHandling;
  }


  /**
   * Gets the table on which the field exists.
   *
   * @return the table
   */
  public TableReference getTable() {
    return table;
  }


  /**
   * Sets the direction to sort the field on.
   *
   * @param direction the direction to set
   * @deprecated Use {@link #direction(Direction)}
   */
  @Deprecated
  public void setDirection(Direction direction) {
    if (immutableDslEnabled())
      throw new UnsupportedOperationException("setDirection method not supported when IMMUTABLE_BUILDERS_ENABLED");
    this.direction = direction;
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected FieldReference deepCopyInternal(DeepCopyTransformation transformer) {
    return new FieldReference(getAlias(), transformer.deepCopy(table), name, direction, nullValueHandling);
  }


  @Override
  protected AliasedField shallowCopy(String aliasName) {
    return new FieldReference(aliasName, table, name, direction, nullValueHandling);
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getTable());
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (table != null) result.append(table).append(".");
    result.append(name);
    if (direction != null && direction != Direction.NONE) result.append(" ").append(direction);
    if (nullValueHandling.isPresent()) result.append(" NULLS ").append(nullValueHandling.get());
    result.append(super.toString());
    return result.toString();
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    // TODO incorrect - permits other types. Can't change this - need to fix existing misuse in subtypes
    if (obj instanceof FieldReference) {
      FieldReference other = (FieldReference) obj;
      return new EqualsBuilder()
                  .appendSuper(super.equals(obj))
                  .append(this.direction, other.direction)
                  .append(this.name, other.name)
                  .append(this.nullValueHandling, other.nullValueHandling)
                  .append(this.table, other.table)
                  .isEquals();
    }
    return false;
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(super.hashCode())
        .append(direction)
        .append(name)
        .append(nullValueHandling)
        .append(table)
        .toHashCode();
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#getImpliedName()
   */
  @Override
  public String getImpliedName() {
    return StringUtils.isBlank(super.getImpliedName()) ? getName() : super.getImpliedName();
  }


  /**
   * sets descending order on this field
   * @return this
   */
  public FieldReference desc() {
    if (immutableDslEnabled()) {
      return new Builder(noTransformation(), this).desc().build();
    } else {
      this.direction = Direction.DESCENDING;
      return this;
    }
  }


  /**
   * sets ascending order on this field
   * @return this
   */
  public FieldReference asc() {
    if (immutableDslEnabled()) {
      return new Builder(noTransformation(), this).asc().build();
    } else {
      this.direction = Direction.ASCENDING;
      return this;
    }
  }


  /**
   * sets null value handling type to last
   * @return this
   */
  public FieldReference nullsLast() {
    if (immutableDslEnabled()) {
      return new Builder(noTransformation(), this).nullsLast().build();
    } else {
      this.nullValueHandling = Optional.of(NullValueHandling.LAST);
      return this;
    }
  }


  /**
   * sets null value handling type to first
   * @return this
   */
  public FieldReference nullsFirst() {
    if (immutableDslEnabled()) {
      return new Builder(noTransformation(), this).nullsFirst().build();
    } else {
      this.nullValueHandling = Optional.of(NullValueHandling.FIRST);
      return this;
    }
  }


  /**
   * sets null value handling type to none
   * @return this
   */
  public FieldReference noNullHandling() {
    if (immutableDslEnabled()) {
      return new Builder(noTransformation(), this).noNullHandling().build();
    } else {
      this.nullValueHandling = Optional.of(NullValueHandling.NONE);
      return this;
    }
  }


  /**
   * Sets the direction to sort the field on.
   *
   * @param direction the direction to set
   * @return this
   */
  public FieldReference direction(Direction direction) {
    return new Builder(noTransformation(), this).direction(direction).build();
  }


  /**
   * Builder for {@link FieldReference}.
   */
  public static final class Builder implements AliasedFieldBuilder {

    private final TableReference table;
    private final String name;

    private String alias;
    private Direction direction = Direction.NONE;
    private Optional<NullValueHandling> nullValueHandling = Optional.empty();

    private Builder(DeepCopyTransformation transformer, FieldReference copyOf) {
      this.alias = copyOf.getAlias();
      this.table = transformer.deepCopy(copyOf.table);
      this.name = copyOf.name;
      this.direction = copyOf.direction;
      this.nullValueHandling = copyOf.nullValueHandling;
    }

    private Builder(TableReference table, String name) {
      this.alias = "";
      this.table = table;
      this.name = name;
    }

    /**
     * sets descending order on this field
     * @return this
     */
    public Builder desc() {
      this.direction = Direction.DESCENDING;
      return this;
    }

    /**
     * sets ascending order on this field
     * @return this
     */
    public Builder asc() {
      this.direction = Direction.ASCENDING;
      return this;
    }

    /**
     * sets null value handling type to last
     * @return this
     */
    public Builder nullsLast() {
      this.nullValueHandling = Optional.of(NullValueHandling.LAST);
      return this;
    }

    /**
     * sets null value handling type to first
     * @return this
     */
    public Builder nullsFirst() {
      this.nullValueHandling = Optional.of(NullValueHandling.FIRST);
      return this;
    }

    /**
     * sets null value handling type to none
     * @return this
     */
    public Builder noNullHandling() {
      this.nullValueHandling = Optional.of(NullValueHandling.NONE);
      return this;
    }

    /**
     * Sets the direction to sort the field on.
     *
     * @param direction the direction to set
     * @return this
     */
    public Builder direction(Direction direction) {
      this.direction = direction;
      return this;
    }

    /**
     * Specifies the alias to use for the field.
     *
     * @param aliasName the name of the alias
     * @return this
     */
    @Override
    public Builder as(String aliasName) {
      this.alias = aliasName;
      return this;
    }

    /**
     * Builds the {@link FieldReference}.
     *
     * @return The field reference.
     */
    @Override
    public FieldReference build() {
      return new FieldReference(alias, table, name, direction, nullValueHandling);
    }
  }
}