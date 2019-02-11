package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getAutoIncrementStartValue;
import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getDataTypeFromColumnComment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;

/**
 * Provides meta data from a PostgreSQL database connection.
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public class PostgreSQLMetaDataProvider extends DatabaseMetaDataProvider {

  /**
   * @param connection The database connection from which meta data should be provided.
   * @param schemaName The name of the schema in which the data is stored.
   */
  public PostgreSQLMetaDataProvider(Connection connection, String schemaName) {
    super(connection, schemaName);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isPrimaryKeyIndex(java.lang.String)
   */
  @Override
  protected boolean isPrimaryKeyIndex(String indexName) {
    return false; //TODO
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#dataTypeFromSqlType(int, java.lang.String, int)
   */
  @Override
  protected DataType dataTypeFromSqlType(int sqlType, String typeName, int width) {
    switch(sqlType) {
    case Types.NUMERIC: //TODO
    default:
      return super.dataTypeFromSqlType(sqlType, typeName, width);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#setAdditionalColumnMetadata(java.lang.String, org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder, java.sql.ResultSet)
   */
  @Override
  protected ColumnBuilder setAdditionalColumnMetadata(String tableName, ColumnBuilder columnBuilder, ResultSet columnMetaData) throws SQLException {
    columnBuilder = super.setAdditionalColumnMetadata(tableName, columnBuilder, columnMetaData);

    // read autonumber from comments
    if (columnBuilder.isAutoNumbered()) {
      int startValue = getAutoIncrementStartValue(columnMetaData.getString(REMARKS));
      columnBuilder.autoNumbered(startValue == -1 ? 1 : startValue);
    }

    // read datatype from comments
    Optional<String> dataTypeComment = getDataTypeFromColumnComment(columnMetaData.getString(REMARKS));
    if(dataTypeComment.isPresent() && dataTypeComment.get().equals("BIG_INTEGER")){
      columnBuilder.dataType(DataType.BIG_INTEGER);
    }

    return columnBuilder;
  }
}
