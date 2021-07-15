package org.alfasoftware.morf.upgrade.db;

import javax.inject.Inject;
import javax.inject.Provider;

import org.alfasoftware.morf.jdbc.SqlDialect;

import com.google.inject.ImplementedBy;

/**
 * Provides {@link SqlDialect} for view definitions stored in DeployedViews table.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
@ImplementedBy(DeployedViewsDefSqlDialectProvider.ImplicitSqlDialect.class)
public interface DeployedViewsDefSqlDialectProvider extends Provider<SqlDialect> {

  /**
   * Default implementation simply returns the implicit dialect.
   */
  class ImplicitSqlDialect implements DeployedViewsDefSqlDialectProvider {

    private final SqlDialect sqlDialect;

    @Inject
    public ImplicitSqlDialect(SqlDialect sqlDialect) {
      this.sqlDialect = sqlDialect;
    }

    @Override
    public SqlDialect get() {
      return sqlDialect;
    }
  }
}
