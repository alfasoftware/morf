package org.alfasoftware.morf.jdbc.h2;

import static org.alfasoftware.morf.sql.SqlUtils.select;

import java.util.List;

import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;

/**
 * Converts parts of Morf's DSL into H2-based SQL.
 *
 * This can be used to produce static SQL from Morf's DSL,
 * but it should be noted that no considerations are made about
 * schema names, or anything related to an actual connection.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public interface H2Sql {

  /**
   * Creates an instance of {@link H2Sql}.
   * @return {@link H2Sql} instance.
   */
  public static H2Sql createH2Sql() {
    return new H2DialectExt().createH2Sql();
  }

  /**
   * Converts given statement to SQL.
   * @param statement Statement to convert.
   * @return Resulting SQL.
   */
  public String sqlFrom(SelectStatement statement);

  /**
   * Converts given statement to SQL.
   * @param statement Statement to convert.
   * @return Resulting SQL.
   */
  public String sqlFrom(SelectFirstStatement statement);

  /**
   * Converts given statement to SQL.
   * @param statement Statement to convert.
   * @return Resulting SQL.
   */
  public List<String> sqlFrom(InsertStatement statement);

  /**
   * Converts given statement to SQL.
   * @param statement Statement to convert.
   * @return Resulting SQL.
   */
  public String sqlFrom(UpdateStatement statement);

  /**
   * Converts given statement to SQL.
   * @param statement Statement to convert.
   * @return Resulting SQL.
   */
  public String sqlFrom(MergeStatement statement);

  /**
   * Converts given statement to SQL.
   * @param statement Statement to convert.
   * @return Resulting SQL.
   */
  public String sqlFrom(DeleteStatement statement);

  /**
   * Converts given statement to SQL.
   * @param statement Statement to convert.
   * @return Resulting SQL.
   */
  public String sqlFrom(TruncateStatement statement);

  /**
   * Converts given statement to SQL.
   * @param statement Statement to convert.
   * @return Resulting SQL.
   */
  public List<String> sqlFrom(Statement statement);

  /**
   * Converts given criterion to SQL.
   * @param criterion Criterion to convert.
   * @return Resulting SQL clause.
   */
  public String sqlFrom(Criterion criterion);
}

/**
 * Implementation of {@link H2Sql}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
class H2DialectExt extends H2Dialect {

  public H2DialectExt() {
    super(null);
  }

  public H2Sql createH2Sql() {
    return new H2Sql() {

      @Override
      public String sqlFrom(SelectStatement statement) {
        return H2DialectExt.super.convertStatementToSQL(statement);
      }

      @Override
      public String sqlFrom(SelectFirstStatement statement) {
        return H2DialectExt.super.convertStatementToSQL(statement);
      }

      @Override
      public List<String> sqlFrom(InsertStatement statement) {
        return H2DialectExt.super.convertStatementToSQL(statement);
      }

      @Override
      public String sqlFrom(UpdateStatement statement) {
        return H2DialectExt.super.convertStatementToSQL(statement);
      }

      @Override
      public String sqlFrom(MergeStatement statement) {
        return H2DialectExt.super.convertStatementToSQL(statement);
      }

      @Override
      public String sqlFrom(DeleteStatement statement) {
        return H2DialectExt.super.convertStatementToSQL(statement);
      }

      @Override
      public String sqlFrom(TruncateStatement statement) {
        return H2DialectExt.super.convertStatementToSQL(statement);
      }

      @Override
      public List<String> sqlFrom(Statement statement) {
        if (statement instanceof InsertStatement) {
          InsertStatement insertStatement = (InsertStatement)statement;
          if (null == insertStatement.getSelectStatement() && insertStatement.getFromTable() != null) {
            statement = insertStatement.shallowCopy().from((TableReference)null).from(select().from(insertStatement.getFromTable())).build();
          }
          if (null == insertStatement.getSelectStatement() && insertStatement.getFromTable() == null && insertStatement.getValues().isEmpty()) {
            return H2DialectExt.super.buildSpecificValueInsert(insertStatement, null, null);
          }
        }
        return H2DialectExt.super.convertStatementToSQL(statement, null, null);
      }

      @Override
      public String sqlFrom(Criterion criterion) {
        return H2DialectExt.super.getSqlFrom(criterion);
      }
    };
  }
}
