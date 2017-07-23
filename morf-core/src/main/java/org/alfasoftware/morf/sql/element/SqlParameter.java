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

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;

/**
 * Provides a representation of a SQL field parameter.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SqlParameter extends AliasedField {

  private final Column metadata;


  /**
   * Generates an iterable of parameters from columns.
   *
   * @param columns table columns.
   * @return parameters matching these columns.
   */
  public static Iterable<SqlParameter> parametersFromColumns(Iterable<Column> columns) {
    return Iterables.transform(columns, new Function<Column, SqlParameter>() {
      @Override
      public SqlParameter apply(Column column) {
        return new SqlParameter(column);
      }
    });
  }


  /**
   * Constructs a new SQL field parameter.
   * @param metadata The parameter metadata
   */
  public SqlParameter(Column metadata) {
    super();
    this.metadata = metadata;
    super.as(metadata.getName());
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new SqlParameter(metadata);
  }


  /**
   * Returns the field metadata for the parameter.
   *
   * @return the field metadata for the parameter.
   */
  public Column getMetadata() {
    return metadata;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", metadata.getName())
        .add("scale", metadata.getScale())
        .add("width", metadata.getWidth())
        .add("type", metadata.getType())
        .toString() + super.toString();
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(metadata.getName())
      .append(metadata.getScale())
      .append(metadata.getWidth())
      .append(metadata.getType())
      .toHashCode();
  }


  /**
   * Functional equality of SQL parameters depends only on the name and data type.
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    SqlParameter other = (SqlParameter) obj;
    return new EqualsBuilder()
      .append(this.metadata.getName(), other.metadata.getName())
      .append(this.metadata.getScale(), other.metadata.getScale())
      .append(this.metadata.getWidth(), other.metadata.getWidth())
      .append(this.metadata.getType(), other.metadata.getType())
      .isEquals();
  }
}
