package org.alfasoftware.morf.jdbc;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

/**
 * Tests for {@link SqlQueryDataSetProducer}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestSqlQueryDataSetProducer {

  @Mock private DataSource dataSource;
  @Mock private SqlDialect sqlDialect;
  @Mock private Connection connection;
  @Mock private Statement statement;

  /**
   * Performs setup common across tests.
   * @throws SQLException
   */
  @Before
  public void setup() throws SQLException {
    MockitoAnnotations.initMocks(this);
    given(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
      .willReturn(statement);
  }


  /**
   * Tests opening the producer.
   *
   * @throws SQLException
   */
  @Test
  public void testOpen() throws SQLException {
    // Given
    Table table = buildTable();
    String query = "select column from table";
    given(dataSource.getConnection())
      .willReturn(connection);

    // When
    SqlQueryDataSetProducer producer = new SqlQueryDataSetProducer(table, query, dataSource, sqlDialect);
    producer.open();

    // Then
    verify(connection).setAutoCommit(false);
  }


  /**
   * Tests closing the producer.
   *
   * @throws SQLException
   */
  @Test
  public void testClose() throws SQLException {
    // Given
    Table table = buildTable();
    String query = "select column from table";
    given(dataSource.getConnection())
      .willReturn(connection);
    given(connection.getAutoCommit()).willReturn(true);

    // When
    SqlQueryDataSetProducer producer = new SqlQueryDataSetProducer(table, query, dataSource, sqlDialect);
    producer.open();
    producer.close();

    // Then
    verify(connection).setAutoCommit(false);
    verify(connection).setAutoCommit(true);
    verify(connection).close();
  }


  /**
   * Tests getting records from the producer.
   *
   * @throws SQLException
   */
  @Test
  public void testRecords() throws SQLException {
    // Given
    Table table = buildTable();
    String query = "select column from table";
    given(dataSource.getConnection())
      .willReturn(connection);
    ResultSet resultSet = mock(ResultSet.class);
    given(statement.executeQuery(query)).willReturn(resultSet);
    given(resultSet.findColumn("Column")).willReturn(1);

    // When
    SqlQueryDataSetProducer producer = new SqlQueryDataSetProducer(table, query, dataSource, sqlDialect);
    producer.open();
    Iterable<Record> records = producer.records("table");

    // Then
    records.iterator();
  }


  private static Table buildTable() {
    return new Table() {

      @Override
      public boolean isTemporary() {
        return false;
      }


      @Override
      public List<Index> indexes() {
        return Lists.newArrayList();
      }


      @Override
      public String getName() {
        return "Table";
      }


      @Override
      public List<Column> columns() {
        return Lists.newArrayList(SchemaUtils.column("Column", DataType.STRING, 20).nullable());
      }

      @Override
      public boolean isPartitioned() { return false; }

      @Override
      public PartitioningRule partitioningRule() {
        return null;
      }

      ;
    };
  }
}
