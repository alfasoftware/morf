package org.alfasoftware.morf.sql.element;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;

public final class PortableSqlExpression extends AliasedField {

  private Map<String, List<AliasedField>> databaseExpressionMap = new HashMap<>();


  public static PortableSqlExpression.Builder builder() {
    return new PortableSqlExpression.Builder();
  }


  public static final class Builder implements AliasedFieldBuilder {

    private Map<String, List<AliasedField>> databaseExpressionMap = new HashMap<>();
    private String alias;


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



  public List<AliasedField> getExpressionForDatabaseType(String databaseTypeIdentifier) {
    List<AliasedField> expression = databaseExpressionMap.get(databaseTypeIdentifier);

    if (expression == null) {
      throw new UnsupportedOperationException("Portable expression not found for database type: " + databaseTypeIdentifier);
    }

    return expression;
  }


  private PortableSqlExpression(Map<String, List<AliasedField>> expressions, String alias) {
    super(alias);
    this.databaseExpressionMap = expressions;
  }


  private PortableSqlExpression(PortableSqlExpression sourceExpression) {
    super(sourceExpression.getAlias());
    this.databaseExpressionMap = sourceExpression.databaseExpressionMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> List.copyOf(e.getValue())));
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PortableSqlExpression)) return false;
    if (!super.equals(o)) return false;
    PortableSqlExpression that = (PortableSqlExpression) o;
    return Objects.equals(databaseExpressionMap, that.databaseExpressionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), databaseExpressionMap);
  }

  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  protected PortableSqlExpression deepCopyInternal(DeepCopyTransformation transformer) {
    return new PortableSqlExpression(PortableSqlExpression.this);
  }
}
