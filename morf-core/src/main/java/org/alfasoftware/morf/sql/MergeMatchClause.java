package org.alfasoftware.morf.sql;

import java.util.Objects;
import java.util.Optional;

import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.alfasoftware.morf.util.ShallowCopyable;

/**
 * Specifies the action to take when records match in a MERGE statement.
 * <p>
 * Currently supports UPDATE operations. Future versions may support other operations (for example DELETE).
 * </p>
 * <p>
 * By default, all matched records will be updated with values from the source.
 * Use {@link Builder#onlyWhere(Criterion)} to conditionally update only records that
 * satisfy additional criteria. This is useful to avoid unnecessary updates when
 * values haven't changed.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public final class MergeMatchClause implements DeepCopyableWithTransformation<MergeMatchClause, MergeMatchClause.Builder>,
    ShallowCopyable<MergeMatchClause, MergeMatchClause.Builder>, Driver, SchemaAndDataChangeVisitable {

  /**
   * The type of action to perform when records match.
   */
  public enum MatchAction {
    UPDATE
  }

  private final MatchAction action;
  private final Optional<Criterion> whereClause;

  /**
   * Creates an update action for matched records.
   *
   * @return a new MergeMatchClause builder
   */
  public static Builder update() {
    return new Builder(MatchAction.UPDATE);
  }


  /**
   * Private constructor - use {@link #update()} to create instances.
   */
  private MergeMatchClause(MatchAction action, Optional<Criterion> whereClause) {
    this.action = action;
    this.whereClause = whereClause;
  }


  /**
   * Constructor for deep copy.
   */
  private MergeMatchClause(MergeMatchClause source, DeepCopyTransformation transformer) {
    this.action = source.action;
    this.whereClause = source.whereClause.map(transformer::deepCopy);
  }


  /**
   * Gets the action type.
   *
   * @return the match action type
   */
  public MatchAction getAction() {
    return action;
  }


  /**
   * Gets the WHERE clause for conditional updates, if specified.
   *
   * @return the WHERE clause, or Optional.empty() if all matched records should
   *         be updated
   */
  public Optional<Criterion> getWhereClause() {
    return whereClause;
  }


  @Override
  public Builder deepCopy(DeepCopyTransformation transformer) {
    return new Builder(new MergeMatchClause(this, transformer));
  }


  @Override
  public Builder shallowCopy() {
    return new Builder(this);
  }


  @Override
  public void drive(ObjectTreeTraverser traverser) {
    whereClause.ifPresent(traverser::dispatch);
  }


  @Override
  public int hashCode() {
    return Objects.hash(action, whereClause);
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    MergeMatchClause other = (MergeMatchClause) obj;
    return action == other.action && Objects.equals(whereClause, other.whereClause);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(action.toString());
    whereClause.ifPresent(c -> sb.append(" WHERE ").append(c));
    return sb.toString();
  }

  /**
   * Builder for {@link MergeMatchClause}.
   */
  public static final class Builder implements org.alfasoftware.morf.util.Builder<MergeMatchClause> {

    private final MatchAction action;
    private Optional<Criterion> whereClause;

    private Builder(MatchAction action) {
      this.action = action;
      this.whereClause = Optional.empty();
    }


    private Builder(MergeMatchClause source) {
      this.action = source.action;
      this.whereClause = source.whereClause;
    }


    /**
     * Specifies a WHERE clause to conditionally apply updates when records match.
     * Only matching records that satisfy this condition will be updated.
     * <p>
     * Example: Update only if rate or description has changed:
     * </p>
     *
     * <pre>
     * MergeMatchClause.update()
     *     .onlyWhere(or(foo.field("rate").neq(new InputField("rate")), foo.field("description").neq(new InputField("description"))))
     * </pre>
     *
     * @param criterion the condition that must be satisfied for the update to occur
     * @return this builder for method chaining
     */
    public Builder onlyWhere(Criterion criterion) {
      this.whereClause = Optional.ofNullable(criterion);
      return this;
    }


    @Override
    public MergeMatchClause build() {
      return new MergeMatchClause(action, whereClause);
    }
  }

  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
    if(whereClause.isPresent()) {
      whereClause.get().accept(visitor);
    }
  }
}