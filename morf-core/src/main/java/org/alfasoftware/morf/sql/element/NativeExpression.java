package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * Provides a representation of native SQL to be used in a {@link Statement}.
 * <p>Native Expressions take in a single string of SQL, for example:
 * {@code new NativeExpression("SELECT * FROM x")}</p>
 * <p>A utility {@link org.alfasoftware.morf.sql.SqlUtils#nativeSql(String)} exists to construct
 * these expressions. The intention is for this to be used within a {@link PortableSqlExpression}
 * to build SQL on the rare occasion that an expression cannot be constructed with existing
 * morf utils. For example:</p>
 * <p>{@code PortableSqlExpression.builder().withExpressionForDatabaseType("PGSQL",
 *    nativeSql("SELECT "),
 *    fieldReference("type"),
 *    nativeSql(" FROM "),
 *    tableRef(TABLE_REF),
 *    nativeSql(" WHERE type = 'T1'"))}</p>
 * @author Copyright (c) Alfa Financial Software Ltd. 2026
 */
public class NativeExpression extends AliasedField{
  /**
   * Native SQL for the expression.
   */
  private final String expression;


  /**
   * Constructs a new {@linkplain NativeExpression} with a string as the SQL.
   * @param expression
   */
  public NativeExpression(String expression) {
    super();
    this.expression = expression;
  }

  /**
   * Constructs a new {@linkplain NativeExpression} with an alias and a string as the SQL.
   * @param alias
   * @param expression
   */
  protected NativeExpression(final String alias, String expression) {
    super(alias);
    this.expression = expression;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#shallowCopy(String)
   */
  @Override
  protected AliasedField shallowCopy(String aliasName) {
    return new NativeExpression(aliasName, this.expression);
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedFieldBuilder deepCopyInternal(DeepCopyTransformation transformer) {
    return new NativeExpression(this.getAlias(), this.getExpression());
  }

  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#accept(SchemaAndDataChangeVisitor)
   */
  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
  }


  /**
   * @return the expression value
   */
  public String getExpression() {
    return expression;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#as(java.lang.String)
   */
  @Override
  public NativeExpression as(String aliasName) {
    return (NativeExpression) super.as(aliasName);
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "\"" + expression + "\"" + super.toString();
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
    NativeExpression other = (NativeExpression) obj;
    return new EqualsBuilder()
        .appendSuper(super.equals(obj))
        .append(this.expression, other.expression)
        .isEquals();
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(super.hashCode())
        .append(this.expression)
        .toHashCode();
  }

}
