package org.alfasoftware.morf.jdbc.oracle;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.junit.Test;

import static org.alfasoftware.morf.metadata.SchemaUtils.*;
import static org.alfasoftware.morf.sql.SqlUtils.*;
import static org.junit.Assert.assertEquals;

public class TestOracleDialectBespokeFunctionality {



    @Test
    public void testConvertStatementToSQL() {
        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3),
                        column("nullField", DataType.STRING, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select(field("someField"), field("otherField"), nullLiteral()).from(tableRef("OtherTable"));

        OracleDialect oracleDialect = new OracleDialect("TEST");
        String result = oracleDialect.addTableFromStatements(table, selectStatement).toString();

        String expectedResult = "[CREATE TABLE TEST.SomeTable (" +
                "someField  NOT NULL, " +
                "otherField  NOT NULL, " +
                "nullField  NOT NULL, " +
                "CONSTRAINT SomeTable_PK PRIMARY KEY (someField) " +
                "USING INDEX (CREATE UNIQUE INDEX TEST.SomeTable_PK ON TEST.SomeTable (someField))) " +
                "PARALLEL NOLOGGING " +
                "AS SELECT " +
                "CAST(someField AS NVARCHAR2(3)) AS someField, " +
                "CAST(otherField AS DECIMAL(3,0)) AS otherField, " +
                "CAST(null AS NVARCHAR2(3)) AS nullField " +
                "FROM TEST.OtherTable, " +
                "ALTER TABLE TEST.SomeTable NOPARALLEL LOGGING, " +
                "ALTER INDEX TEST.SomeTable_PK NOPARALLEL LOGGING, " +
                "COMMENT ON TABLE TEST.SomeTable IS 'REALNAME:[SomeTable]', " +
                "COMMENT ON COLUMN TEST.SomeTable.someField IS 'REALNAME:[someField]/TYPE:[STRING]', " +
                "COMMENT ON COLUMN TEST.SomeTable.otherField IS 'REALNAME:[otherField]/TYPE:[DECIMAL]', " +
                "COMMENT ON COLUMN TEST.SomeTable.nullField IS 'REALNAME:[nullField]/TYPE:[STRING]']";
        assertEquals(expectedResult, result);
    }

    @Test
    public void testDistinctStatement() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select(field("someField"), field("otherField")).from(tableRef("OtherTable"));
        SelectStatement distinctSelectStatement = selectStatement.shallowCopy().distinct().build();

        String result = oracleDialect.addTableFromStatements(table, distinctSelectStatement).toString();

        String expectedResult = "[CREATE TABLE TEST.SomeTable (" +
                "someField  NOT NULL, " +
                "otherField  NOT NULL, " +
                "CONSTRAINT SomeTable_PK PRIMARY KEY (someField) " +
                "USING INDEX (CREATE UNIQUE INDEX TEST.SomeTable_PK ON TEST.SomeTable (someField))) " +
                "PARALLEL NOLOGGING " +
                "AS SELECT DISTINCT " +
                "CAST(someField AS NVARCHAR2(3)) AS someField, " +
                "CAST(otherField AS DECIMAL(3,0)) AS otherField FROM TEST.OtherTable, " +
                "ALTER TABLE TEST.SomeTable NOPARALLEL LOGGING, " +
                "ALTER INDEX TEST.SomeTable_PK NOPARALLEL LOGGING, " +
                "COMMENT ON TABLE TEST.SomeTable IS 'REALNAME:[SomeTable]', " +
                "COMMENT ON COLUMN TEST.SomeTable.someField IS 'REALNAME:[someField]/TYPE:[STRING]', " +
                "COMMENT ON COLUMN TEST.SomeTable.otherField IS 'REALNAME:[otherField]/TYPE:[DECIMAL]']";
        assertEquals(expectedResult, result);
    }


    @Test
    public void testAllStatement() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select().from(tableRef("OtherTable"));
        SelectStatement distinctSelectStatement = selectStatement.shallowCopy().distinct().build();

        String result = oracleDialect.addTableFromStatements(table, distinctSelectStatement).toString();

        String expectedResult = "[CREATE TABLE TEST.SomeTable (" +
                "someField  NOT NULL, " +
                "otherField  NOT NULL, " +
                "CONSTRAINT SomeTable_PK PRIMARY KEY (someField) " +
                "USING INDEX (CREATE UNIQUE INDEX TEST.SomeTable_PK ON TEST.SomeTable (someField))) " +
                "PARALLEL NOLOGGING " +
                "AS SELECT DISTINCT * FROM TEST.OtherTable, " +
                "ALTER TABLE TEST.SomeTable NOPARALLEL LOGGING, " +
                "ALTER INDEX TEST.SomeTable_PK NOPARALLEL LOGGING, " +
                "COMMENT ON TABLE TEST.SomeTable IS 'REALNAME:[SomeTable]', " +
                "COMMENT ON COLUMN TEST.SomeTable.someField IS 'REALNAME:[someField]/TYPE:[STRING]', " +
                "COMMENT ON COLUMN TEST.SomeTable.otherField IS 'REALNAME:[otherField]/TYPE:[DECIMAL]']";
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUpdateStatement() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select().from(tableRef("OtherTable"));
        SelectStatement updateSelectStatement = selectStatement.shallowCopy().forUpdate().build();

        String result = oracleDialect.addTableFromStatements(table, updateSelectStatement).toString();

        String expectedResult = "[CREATE TABLE TEST.SomeTable (" +
                "someField  NOT NULL, " +
                "otherField  NOT NULL, " +
                "CONSTRAINT SomeTable_PK PRIMARY KEY (someField) " +
                "USING INDEX (CREATE UNIQUE INDEX TEST.SomeTable_PK ON TEST.SomeTable (someField))) " +
                "PARALLEL NOLOGGING " +
                "AS SELECT * FROM TEST.OtherTable " +
                "FOR UPDATE, " +
                "ALTER TABLE TEST.SomeTable NOPARALLEL LOGGING, " +
                "ALTER INDEX TEST.SomeTable_PK NOPARALLEL LOGGING, " +
                "COMMENT ON TABLE TEST.SomeTable IS 'REALNAME:[SomeTable]', " +
                "COMMENT ON COLUMN TEST.SomeTable.someField IS 'REALNAME:[someField]/TYPE:[STRING]', " +
                "COMMENT ON COLUMN TEST.SomeTable.otherField IS 'REALNAME:[otherField]/TYPE:[DECIMAL]']";
        assertEquals(expectedResult, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateForDistinctStatement() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select().from(tableRef("OtherTable"));
        SelectStatement distinctSelectStatement = selectStatement.shallowCopy().forUpdate().distinct().build();

        String result = oracleDialect.addTableFromStatements(table, distinctSelectStatement).toString();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgumentExeptionIsThrown() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3),
                        column("nullField", DataType.STRING, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        oracleDialect.convertStatementToSQL((SelectStatement) null);
    }


}
