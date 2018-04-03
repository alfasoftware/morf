package org.alfasoftware.morf.sql;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Direction;
import org.alfasoftware.morf.sql.element.FieldReference;

import com.google.common.collect.FluentIterable;

/**
 * Utility methods for SQL classes.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
class SqlInternalUtils {

  /**
   * Sets the fields in an ORDER BY to use ascending order if not specified.
   *
   * @param orderBys The order by criteria
   */
  static Iterable<AliasedField> transformOrderByToAscending(Iterable<AliasedField> orderBys) {
    return FluentIterable.from(orderBys).transform(o -> transformFieldReference(o)).toList();
  }


  /**
   * Sets the fields in an ORDER BY to use ascending order if not specified.
   *
   * This method will be removed when statement immutability is turned on permanently.
   */
  @Deprecated
  static void defaultOrderByToAscending(Iterable<AliasedField> orderBys) {
    for (AliasedField currentField : orderBys) {
      if (currentField instanceof FieldReference && ((FieldReference) currentField).getDirection() == Direction.NONE) {
        ((FieldReference) currentField).setDirection(Direction.ASCENDING);
      }
    }
  }


  private static AliasedField transformFieldReference(AliasedField currentField) {
    if (currentField instanceof FieldReference && ((FieldReference) currentField).getDirection() == Direction.NONE) {
      return ((FieldReference) currentField).direction(Direction.ASCENDING);
    }
    return currentField;
  }

}