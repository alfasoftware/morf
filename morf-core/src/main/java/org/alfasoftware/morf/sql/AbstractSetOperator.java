package org.alfasoftware.morf.sql;

/**
 * Common behaviour of set operators.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
abstract class AbstractSetOperator {
  /**
   * Don't allow {@code null} references to {@linkplain SelectStatement}.
   *
   * @param parentSelect the select statement to be validated.
   * @param childSelect the select statement to be validated.
   */
  void validateNotNull(SelectStatement parentSelect, SelectStatement childSelect) throws IllegalArgumentException {
    if (parentSelect == null || childSelect == null) {
      throw new IllegalArgumentException("Select statements cannot be null");
    }
  }


  /**
   * Don't allow {@code childSelect} have a different number of fields from
   * {@code parentSelect}.
   * <p>
   * The column names from the parent select statement are used as the column
   * names for the results returned. Selected columns listed in corresponding
   * positions of each SELECT statement should have the same data type.
   * </p>
   *
   * @param parentSelect the select statement to be compared against.
   * @param childSelect the select statement to be validated.
   */
  void validateFields(SelectStatement parentSelect, SelectStatement childSelect) throws IllegalArgumentException {
    if (parentSelect.getFields().size() != childSelect.getFields().size()) {
      throw new IllegalArgumentException("A set operator requires selecting the same number of fields on both select statements");
    }
  }
}
