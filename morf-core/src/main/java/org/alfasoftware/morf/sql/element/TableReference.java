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

import org.alfasoftware.morf.sql.TempTransitionalBuilderWrapper;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Class which represents a table in an SQL statement. Each table can
 * have an alias associated to it.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class TableReference implements DeepCopyableWithTransformation<TableReference,
                                       Builder<TableReference>> {

  /**
   * The schema which holds this table (optional).
   */
  private final String schemaName;

  /**
   * The name of the table. TODO make final (see {@link #setName(String)).
   */
  private String name;

  /**
   * The alias to use for the table (optional). TODO make final (see {@link #as(String)}).
   */
  private String alias;

  /**
   * If set to true defines the table as a temporary table.
   */
  private final boolean temporary;

  /**
   * Constructor used to create the deep copy
   *
   * @param sourceTable the source table to copy the values from
   */
  private TableReference(TableReference sourceTable, String alias) {
    this.name = sourceTable.name;
    this.alias = alias;
    this.schemaName = sourceTable.schemaName;
    this.temporary = sourceTable.temporary;
  }

  /**
   * Construct a new table with a given name.
   *
   * @param name the name of the table
   */
  public TableReference(String name) {
    this.name = name;
    this.alias = "";
    this.schemaName = null;
    this.temporary = false;
  }


  /**
   * Construct a new table with a given name in a given schema.
   * Specifies the schema which contains this table.
   *
   * @param schemaName the schema which contains this table
   * @param tableName the name of the table
   */
  public TableReference(String schemaName, String tableName) {
    this.name = tableName;
    this.schemaName = schemaName;
    this.alias = "";
    this.temporary = false;
  }


  /**
   * Constructs a new table with a given name.
   * Specifies whether the table is temporary.
   *
   * @param tableName
   * @param temporary
   */
  public TableReference(String tableName, boolean temporary) {
    this.name = tableName;
    this.schemaName = null;
    this.alias = "";
    this.temporary = temporary;
  }


  /**
   * Specifies the alias to use for the table.
   *
   * @param aliasName the name of the alias
   * @return an updated {@link TableReference} (this will not be a new object)
   */
  public TableReference as(String aliasName) {
    if (AliasedField.immutableDslEnabled()) {
      return new TableReference(this, aliasName);
    } else {
      this.alias = aliasName;
      return this;
    }
  }


  /**
   * Get the name of the table
   *
   * @return the name
   */
  public String getName() {
    return name;
  }


  /**
   * Get the alias for the table
   *
   * @return the alias
   */
  public String getAlias() {
    return alias;
  }


  /**
   * Get the schema which contains this table.
   *
   * @return the schema name
   */
  public String getSchemaName() {
    return schemaName;
  }


  /**
   * Indicates whether the table is temporary.
   */
  public boolean isTemporary() {
    return temporary;
  }


  /**
   * @param name the name to set
   * @deprecated Do not modify {@link TableReference} instances. This will be removed very soon.
   */
  @Deprecated
  public void setName(String name) {
    this.name = name;
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<TableReference> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(deepCopy());
  }


  /**
   * Create a deep copy of this table.
   *
   * @return TableReference a deep copy for this table
   */
  public TableReference deepCopy() {
    return new TableReference(this, TableReference.this.alias);
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if(isTemporary()) result.append("/*Temporary table*/ ");
    if (!StringUtils.isEmpty(schemaName)) result.append(schemaName).append(".");
    result.append(name);
    if (!StringUtils.isEmpty(alias)) result.append(" ").append(alias);
    return result.toString();
  }


  /**
   * @param fieldName the name of the field
   * @return reference to a field on this table.
   */
  public FieldReference field(String fieldName) {
    return FieldReference.field(this, fieldName).build();
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (obj == this)
      return true;
    // TODO incorrect - permits other types. Can't change this - need to fix existing misuse in subtypes
    if (!(obj instanceof TableReference))
      return false;
    TableReference rhs = (TableReference)obj;
    return new EqualsBuilder()
        .append(schemaName, rhs.schemaName)
        .append(name, rhs.name)
        .append(alias, rhs.alias)
        .append(temporary, rhs.temporary)
        .isEquals();
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder(1559, 887)
        .append(schemaName)
        .append(name)
        .append(alias)
        .append(temporary)
        .toHashCode();
  }
}