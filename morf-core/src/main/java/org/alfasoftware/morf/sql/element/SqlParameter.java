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
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.sql.SqlUtils;
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

  private final String name;
  private final DataType type;
  private final int width;
  private final int scale;


  /**
   * Constructs a new SQL named parameter.  Usage:
   *
   * <pre>parameter("name").type(DataType.INTEGER).build();
parameter("name").type(DataType.STRING).width(10).build();
parameter("name").type(DataType.DECIMAL).width(13,2).build();</pre>
   *
   * @param name the parameter name.
   * @return Builder.
   */
  public static NeedsType parameter(String name) {
    return new BuilderImpl(name);
  }


  /**
   * Constructs a new SQL named parameter from a column.
   *
   * @param column the parameter column.
   * @return Builder.
   */
  public static Builder parameter(Column column) {
    return parameter(column.getName())
        .type(column.getType())
        .width(column.getWidth(), column.getScale());
  }


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
   *
   * @param metadata The parameter metadata
   * @deprecated Use {@link #parameter(String)}.
   */
  @Deprecated
  public SqlParameter(Column metadata) {
    this(metadata.getName(), metadata.getType(), metadata.getWidth(), metadata.getScale());
  }


  protected SqlParameter(String name, DataType dataType, int width, int scale) {
    super(name);
    this.name = name;
    this.type = dataType;
    this.width = width;
    this.scale = scale;
  }


  private SqlParameter(String alias, String name, DataType dataType, int width, int scale) {
    super(alias);
    this.name = name;
    this.type = dataType;
    this.width = width;
    this.scale = scale;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected SqlParameter deepCopyInternal(DeepCopyTransformation transformer) {
    return new SqlParameter(name, type, width, scale);
  }


  @Override
  protected AliasedField shallowCopy(String aliasName) {
    return new SqlParameter(aliasName, name, type, width, scale);
  }


  /**
   * Support for {@link SqlUtils#parameter(String)}. Creates
   * a duplicate with a new width and size.
   *
   * @param width The width.
   * @param scale The scale.
   * @return The copy.
   */
  protected SqlParameter withWidth(int width, int scale) {
    return new SqlParameter(name, type, width, scale);
  }


  /**
   * Returns the field metadata for the parameter.
   *
   * @return the field metadata for the parameter.
   */
  public Column getMetadata() {
    return SchemaUtils.column(name, type, width, scale);
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("scale", scale)
        .add("width", width)
        .add("type", type)
        .toString() + super.toString();
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(super.hashCode())
        .append(name)
        .append(scale)
        .append(width)
        .append(type)
        .toHashCode();
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SqlParameter other = (SqlParameter) obj;
    return new EqualsBuilder()
        .appendSuper(super.equals(obj))
        .append(name, other.name)
        .append(scale, other.scale)
        .append(width, other.width)
        .append(type, other.type)
        .isEquals();
  }


  /**
   * Builder stage for {@link SqlParameter}.
   */
  public interface NeedsType {

    /**
     * Specifies the data type for the parameter.
     *
     * @param dataType The data type
     * @return the next phase of the parameter builder.
     */
    public Builder type(DataType dataType);

  }


  /**
   * Builder stage for {@link SqlParameter}.
   */
  public interface Builder extends NeedsType {

    /**
     * Specifies the width of the parameter.
     *
     * @param width The width.
     * @return Builder.
     */
    public Builder width(int width);

    /**
     * Specifies the width and scale of the parameter.
     *
     * @param width The width.
     * @param scale The scale.
     * @return Builder.
     */
    public Builder width(int width, int scale);

    /**
     * Builds the {@link SqlParameter}.
     *
     * @return The parameter.
     */
    public SqlParameter build();
  }


  /**
   * Builder implementation for {@link SqlParameter}.
   */
  private static final class BuilderImpl implements Builder {

    private final String name;
    private DataType dataType;
    private int width;
    private int scale;

    private BuilderImpl(String name) {
      super();
      this.name = name;
    }

    @Override
    public BuilderImpl type(DataType dataType) {
      this.dataType = dataType;
      return this;
    }

    @Override
    public BuilderImpl width(int width) {
      this.width = width;
      this.scale = 0;
      return this;
    }

    @Override
    public BuilderImpl width(int width, int scale) {
      this.width = width;
      this.scale = scale;
      return this;
    }

    @Override
    public SqlParameter build() {
      return new SqlParameter(name, dataType, width, scale);
    }
  }
}