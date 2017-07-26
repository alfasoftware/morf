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

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.base.Optional;

/**
 * A field is used by a query to represent a column within the database. This class
 * has no concept of the type or representation of a field, only its name, alias and
 * the table on which it exists.
 *
 * <h3>Examples of use:</h3>
 *
 * <p>Create a field with a given name:</p>
 * <blockquote><pre>
 *    Field newField = new Field("agreementnumber");</pre></blockquote>
 *
 * <p>Create a field with a given name and alias of "bob". This is equivalent to "agreementnumber AS bob" in SQL:</p>
 * <blockquote><pre>
 *    Field newField = new Field("agreementnumber", "bob");</pre></blockquote>
 *
 * <p>Create a field which is sorted descending:</p>
 * <blockquote><pre>
 *    Field newField = new Field("agreementnumber", Direction.DESCENDING);</pre></blockquote>
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
   * The direction to use when this field is in an ORDER BY statement
   */
  private Direction direction;

  /**
   * Handling of null values when executing ORDER BY statement
   */
  private Optional<NullValueHandling> nullValueHandling = Optional.absent();


  /**
   * Constructor used to create the deep copy of this field reference
   *
   * @param sourceField the field reference to create the copy from
   * @param transformer The transformation to be executed during the copy
   */
  protected FieldReference(FieldReference sourceField, DeepCopyTransformation transformer) {
    super();

    this.table = transformer.deepCopy(sourceField.table);
    this.name = sourceField.name;
    this.direction = sourceField.direction;
    this.nullValueHandling = sourceField.nullValueHandling;
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
    this(table, name, direction, Optional.of(nullValueHandling));
  }


  private FieldReference(TableReference table, String name, Direction direction, Optional<NullValueHandling> nullValueHandling) {
    super();
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
    this(table, name, direction, Optional.<NullValueHandling>absent());
  }


  /**
   * Constructs a new field with an alias on a given table.
   *
   * @param table the table on which the field exists
   * @param name the name of the field
   */
  public FieldReference(TableReference table, String name) {
    this(table, name, Direction.NONE);
  }


  /**
   * Constructs a new field with a given name
   *
   * @param name the name of the field
   */
  public FieldReference(String name) {
    this(null, name);
  }


  /**
   * Constructs a new field with a name and a sort direction.
   *
   * @param name the name of the field
   * @param direction the sort direction for the field
   */
  public FieldReference(String name, Direction direction) {
    this(null, name, direction);
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
   */
  public void setDirection(Direction direction) {
    this.direction = direction;
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new FieldReference(this,transformer);
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
    if (obj instanceof FieldReference) {
      FieldReference other = (FieldReference) obj;
      return new EqualsBuilder()
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
    this.direction = Direction.DESCENDING;
    return this;
  }


  /**
   * sets ascending order on this field
   * @return this
   */
  public FieldReference asc() {
    this.direction = Direction.ASCENDING;
    return this;
  }


  /**
   * sets null value handling type to last
   * @return this
   */
  public FieldReference nullsLast() {
    this.nullValueHandling = Optional.of(NullValueHandling.LAST);
    return this;
  }


  /**
   * sets null value handling type to first
   * @return this
   */
  public FieldReference nullsFirst() {
    this.nullValueHandling = Optional.of(NullValueHandling.FIRST);
    return this;
  }

  /**
   * sets null value handling type to none
   * @return this
   */
  public FieldReference noNullHandling() {
    this.nullValueHandling = Optional.of(NullValueHandling.NONE);
    return this;
  }
}
