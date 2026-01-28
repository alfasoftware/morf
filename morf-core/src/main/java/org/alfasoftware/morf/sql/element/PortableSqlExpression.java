package org.alfasoftware.morf.sql.element;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;


/**
 * Portable expressions allow for SQL code to be freely defined by combining {@link AliasedField}s.
 * <p>These portable expressions should only be used when it is absolutely necessary, for instance when a specific expression</p>
 * <p>Expressions are mapped to database type identifiers, and distinct statements are required for each supported database
 * is required where there is no support (nor planned support) within morf.</p>
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2026
 */
public final class PortableSqlExpression extends AliasedField {

  private final Map<String, List<AliasedField>> databaseExpressionMap;

  /**
   * Starts a new PortableSqlExpression builder
   */
  public static PortableSqlExpression.Builder builder() {
    return new PortableSqlExpression.Builder();
  }


  public static final class Builder implements AliasedFieldBuilder {

    private final Map<String, List<AliasedField>> databaseExpressionMap = new HashMap<>();
    private String alias;


    /**
     * Adds a portable expression for a single database type.
     *
     * @param databaseTypeIdentifier this identifier should match the DatabaseType.IDENTIFIER value
     * @param arguments              the {@link AliasedField} arguments for the expression
     * @return the builder
     */
    public PortableSqlExpression.Builder withExpressionForDatabaseType(String databaseTypeIdentifier, AliasedField... arguments) {
      List<AliasedField> expressions = Arrays.asList(arguments);
      databaseExpressionMap.put(databaseTypeIdentifier, expressions);

      return this;
    }


    @Override
    public PortableSqlExpression.Builder as(String alias) {
      this.alias = alias;
      return this;
    }

    public PortableSqlExpression build() {
      return new PortableSqlExpression(databaseExpressionMap, alias);
    }
  }


  /**
   * @return the expression and arguments for the provided databaseTypeIdentifier. Throws an UnsupportedOperationException
   * if no function is found.
   */
  public List<AliasedField> getExpressionForDatabaseType(String databaseTypeIdentifier) {
    List<AliasedField> expression = databaseExpressionMap.get(databaseTypeIdentifier);

    if (expression == null) {
      throw new UnsupportedOperationException("Portable expression not found for database type: " + databaseTypeIdentifier);
    }

    return expression;
  }


  /**
   * Constructs a new {@link PortableSqlExpression} with a map of expressions for each database type and alias.
   * @param expressions
   * @param alias
   */
  private PortableSqlExpression(Map<String, List<AliasedField>> expressions, String alias) {
    super(alias);
    this.databaseExpressionMap = expressions;
  }


  /**
   * Constructs a new {@link PortableSqlExpression} from an existing {@link PortableSqlExpression}
   * @param sourceExpression
   */
  private PortableSqlExpression(PortableSqlExpression sourceExpression) {
    super(sourceExpression.getAlias());
    this.databaseExpressionMap = sourceExpression.databaseExpressionMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> List.copyOf(e.getValue())));
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PortableSqlExpression)) return false;
    if (!super.equals(o)) return false;
    PortableSqlExpression that = (PortableSqlExpression) o;
    return Objects.equals(databaseExpressionMap, that.databaseExpressionMap);
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), databaseExpressionMap);
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#accept(SchemaAndDataChangeVisitor)
   */
  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected PortableSqlExpression deepCopyInternal(DeepCopyTransformation transformer) {
    return new PortableSqlExpression(PortableSqlExpression.this);
  }
}
