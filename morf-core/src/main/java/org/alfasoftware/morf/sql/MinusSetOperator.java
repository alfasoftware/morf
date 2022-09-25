package org.alfasoftware.morf.sql;

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;

/**
 * Class which encapsulates the generation of a MINUS set operator. It
 * eliminates any rows from the parent select statement which do not exist in
 * the child select statement.
 * <p>
 * This class cannot be instantiated directly. Instead, use the
 * {@linkplain SelectStatement#minus(SelectStatement)} which encapsulate this
 * class existence.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class MinusSetOperator extends AbstractSetOperator implements SetOperator {

  private final SelectStatement selectStatement;

  /**
   * Creates a new MINUS set operation between the {@code parentSelect} and
   * {@code childSelect}, either removing duplicate records.
   * <p>
   * A reference to the {@code parentSelect} is not maintained as the only way
   * to construct this class is via the parent select itself. Its reference is
   * necessary only for validation purposes.
   * </p>
   */
  MinusSetOperator(SelectStatement parentSelect, SelectStatement childSelect) {
    validateNotNull(parentSelect, childSelect);
    validateFields(parentSelect, childSelect);

    this.selectStatement = childSelect;
  }


  /**
   * Constructor used to create a deep copy of a MINUS statement.
   *
   * @param childSelect the second part of the MINUS statement
   */
  private MinusSetOperator(SelectStatement childSelect) {
    this.selectStatement = childSelect;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getSelectStatement());
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<SetOperator> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.<SetOperator>wrapper(new MinusSetOperator(transformer.deepCopy(getSelectStatement())));
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.sql.SetOperator#getSelectStatement()
   */
  @Override
  public SelectStatement getSelectStatement() {
    return selectStatement;
  }


  /**
   * @see org.alfasoftware.morf.sql.SchemaAndDataChangeVisitable#accept(org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor)
   */
  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
    selectStatement.accept(visitor);
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (selectStatement == null ? 0 : selectStatement.hashCode());
    return result;
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    MinusSetOperator other = (MinusSetOperator) obj;
    if (selectStatement == null) {
      if (other.selectStatement != null) return false;
    } else if (!selectStatement.equals(other.selectStatement)) return false;
    return true;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "MINUS " + selectStatement;
  }
}