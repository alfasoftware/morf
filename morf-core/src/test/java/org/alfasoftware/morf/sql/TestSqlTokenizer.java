package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.DQUOTED;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.DSTRING;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.IDENTIFIER;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.ILLEGAL;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.MCOMMENT;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.NUMBER;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.OPERATOR;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.SCOMMENT;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.STRING;
import static org.alfasoftware.morf.sql.SqlTokenizer.TokenType.WHITESPACE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.alfasoftware.morf.sql.SqlTokenizer.TokenType;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestSqlTokenizer {

  private static final Pair<TokenType, String> SQL_SELECT = Pair.of(IDENTIFIER, "SELECT");
  private static final Pair<TokenType, String> SQL_FROM = Pair.of(IDENTIFIER, "FROM");
  private static final Pair<TokenType, String> SQL_WHERE = Pair.of(IDENTIFIER, "WHERE");
  private static final Pair<TokenType, String> SQL_TABLE = Pair.of(IDENTIFIER, "table");
  private static final Pair<TokenType, String> SQL_COLUMN = Pair.of(IDENTIFIER, "column");
  private static final Pair<TokenType, String> SQL_SCHEMA = Pair.of(IDENTIFIER, "Schema");
  private static final Pair<TokenType, String> SQL_TABLE_NAME = Pair.of(IDENTIFIER, "table_name");
  private static final Pair<TokenType, String> SQL_MAX = Pair.of(IDENTIFIER, "MAX");
  private static final Pair<TokenType, String> SQL_ID = Pair.of(IDENTIFIER, "id");
  private static final Pair<TokenType, String> SQL_ONE = Pair.of(IDENTIFIER, "one");
  private static final Pair<TokenType, String> SQL_TWO = Pair.of(IDENTIFIER, "two");
  private static final Pair<TokenType, String> SQL_THREE = Pair.of(IDENTIFIER, "three");
  private static final Pair<TokenType, String> SQL_NULL = Pair.of(IDENTIFIER, "null");
  private static final Pair<TokenType, String> SQL_UNDERSCORE = Pair.of(IDENTIFIER, "_");

  private static final Pair<TokenType, String> SQL_SPACE = Pair.of(WHITESPACE, " ");
  private static final Pair<TokenType, String> SQL_CRNL = Pair.of(WHITESPACE, "\r\n");
  private static final Pair<TokenType, String> SQL_COMMA = Pair.of(OPERATOR, ",");
  private static final Pair<TokenType, String> SQL_DOT = Pair.of(OPERATOR, ".");
  private static final Pair<TokenType, String> SQL_STAR = Pair.of(OPERATOR, "*");
  private static final Pair<TokenType, String> SQL_PLUS = Pair.of(OPERATOR, "+");
  private static final Pair<TokenType, String> SQL_MINUS = Pair.of(OPERATOR, "-");
  private static final Pair<TokenType, String> SQL_SLASH = Pair.of(OPERATOR, "/");
  private static final Pair<TokenType, String> SQL_GT = Pair.of(OPERATOR, ">");
  private static final Pair<TokenType, String> SQL_CLOSEBRACKET = Pair.of(OPERATOR, ")");
  private static final Pair<TokenType, String> SQL_OPENBRACKET = Pair.of(OPERATOR, "(");

  private static final Pair<TokenType, String> SQL_0 = Pair.of(NUMBER, "0");
  private static final Pair<TokenType, String> SQL_1 = Pair.of(NUMBER, "1");
  private static final Pair<TokenType, String> SQL_3 = Pair.of(NUMBER, "3");
  private static final Pair<TokenType, String> SQL_4 = Pair.of(NUMBER, "4");
  private static final Pair<TokenType, String> SQL_7 = Pair.of(NUMBER, "7");

  private List<Pair<TokenType, String>> runTokenizeSqlQuery(String databaseType, String query) {
    final ImmutableList.Builder<Pair<TokenType, String>> tokens = ImmutableList.builder();
    final AtomicInteger nextPositionCounter = new AtomicInteger(0);

    SqlTokenizer.tokenizeSqlQuery(databaseType, query, t -> {
      final int nextPosition = nextPositionCounter.get();
      assertThat(t.getPosition(), equalTo(nextPosition));
      assertThat(t.getString().length(), greaterThan(0));
      nextPositionCounter.set(nextPosition + t.getString().length());
      tokens.add(Pair.of(t.getType(), t.getString()));
    });
    return tokens.build();
  }


  @Test
  public void testTokenizeIdentifiersNumbers() {
    assertThat(
      runTokenizeSqlQuery(null, "SELECT column FROM table"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_COLUMN, SQL_SPACE,
        SQL_FROM, SQL_SPACE, SQL_TABLE)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT one,two, 3 three FROM Schema.table_name"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_ONE, SQL_COMMA, SQL_TWO, SQL_COMMA, SQL_SPACE, SQL_3, SQL_SPACE, SQL_THREE, SQL_SPACE,
        SQL_FROM, SQL_SPACE, SQL_SCHEMA, SQL_DOT, SQL_TABLE_NAME)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT 4.7 _, 1.925e-32 FROM Schema.table_name"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_4, SQL_DOT, SQL_7, SQL_SPACE, SQL_UNDERSCORE, SQL_COMMA, SQL_SPACE,
        SQL_1, SQL_DOT, Pair.of(NUMBER, "925"), Pair.of(IDENTIFIER, "e"), SQL_MINUS, Pair.of(NUMBER, "32"), SQL_SPACE,
        SQL_FROM, SQL_SPACE, SQL_SCHEMA, SQL_DOT, SQL_TABLE_NAME)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT (1+1) / 7 FROM _ WHERE column > 0"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_OPENBRACKET, SQL_1, SQL_PLUS, SQL_1, SQL_CLOSEBRACKET, SQL_SPACE, SQL_SLASH, SQL_SPACE, SQL_7, SQL_SPACE,
        SQL_FROM, SQL_SPACE, SQL_UNDERSCORE, SQL_SPACE,
        SQL_WHERE, SQL_SPACE, SQL_COLUMN, SQL_SPACE, SQL_GT, SQL_SPACE, SQL_0)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT *, 1 + column FROM table"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_STAR, SQL_COMMA, SQL_SPACE, SQL_1, SQL_SPACE, SQL_PLUS, SQL_SPACE, SQL_COLUMN, SQL_SPACE,
        SQL_FROM, SQL_SPACE, SQL_TABLE)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT MAX(id) FROM table"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_MAX, SQL_OPENBRACKET, SQL_ID, SQL_CLOSEBRACKET, SQL_SPACE,
        SQL_FROM, SQL_SPACE, SQL_TABLE)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT column FROM v$sql"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_COLUMN, SQL_SPACE,
        SQL_FROM, SQL_SPACE, Pair.of(IDENTIFIER, "v$sql"))));

    //
    // parsing edge cases

    assertThat(
      runTokenizeSqlQuery(null, "SELECT column FROM $sql"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_COLUMN, SQL_SPACE,
        SQL_FROM, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "sql"))));
  }


  @Test
  public void testTokenizeSingleQuotes() {
    assertThat(
      runTokenizeSqlQuery(null, "SELECT '' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "''"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT 'abc' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "'abc'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT 'a''c' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "'a''c'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT 'a'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "'a'"))));

    //
    // parsing edge cases

    assertThat(
      runTokenizeSqlQuery(null, "SELECT 'a"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "'"), Pair.of(IDENTIFIER, "a"))));

    //
    // ORACLE-specific without ORACLE

    assertThat(
      runTokenizeSqlQuery(null, "SELECT n'abc' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "n"), Pair.of(STRING, "'abc'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT N'abc' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "N"), Pair.of(STRING, "'abc'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT n"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "n"))));

    //
    // ORACLE-specific on ORACLE

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT n'abc' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "n'abc'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT N'abc' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "N'abc'"), SQL_SPACE, SQL_FROM)));

    //
    // ORACLE-specific on ORACLE parsing edge cases

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT N"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "N"))));

    //
    // PGSQL-specific without PGSQL

    assertThat(
      runTokenizeSqlQuery(null, "SELECT b'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "b"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT B'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "B"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT x'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "x"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT X'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "X"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT u&'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "u"), Pair.of(OPERATOR, "&"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT U&'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "U"), Pair.of(OPERATOR, "&"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT u'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "u"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT U'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "U"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT e'a\\'c' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "e"), Pair.of(STRING, "'a\\'"), Pair.of(IDENTIFIER, "c"), Pair.of(ILLEGAL, "'"), SQL_SPACE, Pair.of(IDENTIFIER, "FROM"))));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT E'a\\'c' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "E"), Pair.of(STRING, "'a\\'"), Pair.of(IDENTIFIER, "c"), Pair.of(ILLEGAL, "'"), SQL_SPACE, Pair.of(IDENTIFIER, "FROM"))));

    //
    // PGSQL-specific on PGSQL

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT b'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "b'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT B'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "B'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT x'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "x'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT X'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "X'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT u&'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "u&'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT U&'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "U&'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT u'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "u"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT U'ac' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "U"), Pair.of(STRING, "'ac'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT e'a\\'c' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "e'a\\'c'"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT E'a\\'c' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "E'a\\'c'"), SQL_SPACE, SQL_FROM)));

    //
    // PGSQL-specific on PGSQL parsing edge cases

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT b"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "b"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT X"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "X"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT u"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "u"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT U&"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "U"), Pair.of(OPERATOR, "&"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT e"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "e"))));

    //
    // newlines inside of single-quoted strings
    // note: newlines are allowed inside single-quoted string literals (PGSQL, ORACLE)

    assertThat(
      runTokenizeSqlQuery(null, "SELECT 'newlines\nare\rallowed' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "'newlines\nare\rallowed'"), SQL_SPACE, SQL_FROM)));

  }


  @Test
  public void testTokenizeDoubleQuotedIdentifiers() {
    assertThat(
      runTokenizeSqlQuery(null, "SELECT \"column\" FROM \"table\""),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DQUOTED, "\"column\""), SQL_SPACE,
        SQL_FROM, SQL_SPACE, Pair.of(DQUOTED, "\"table\""))));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT \"1\" FROM \"Schema\".\"table_name\""),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DQUOTED, "\"1\""), SQL_SPACE,
        SQL_FROM, SQL_SPACE, Pair.of(DQUOTED, "\"Schema\""), SQL_DOT, Pair.of(DQUOTED, "\"table_name\""))));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT null FROM \"table\"\"name\""),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_NULL, SQL_SPACE,
        SQL_FROM, SQL_SPACE, Pair.of(DQUOTED, "\"table\"\"name\""))));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT column FROM \"v$sql\""),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, SQL_COLUMN, SQL_SPACE,
        SQL_FROM, SQL_SPACE, Pair.of(DQUOTED, "\"v$sql\""))));

    //
    // edge cases

    assertThat(
      runTokenizeSqlQuery(null, "SELECT \"column FROM table"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "\""), Pair.of(IDENTIFIER, "column"), SQL_SPACE,
        SQL_FROM, SQL_SPACE, Pair.of(IDENTIFIER, "table"))));

    //
    // PGSQL-specific without PGSQL

    assertThat(
      runTokenizeSqlQuery(null, "SELECT u&\"ac\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "u"), Pair.of(OPERATOR, "&"), Pair.of(DQUOTED, "\"ac\""), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT U&\"ac\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "U"), Pair.of(OPERATOR, "&"), Pair.of(DQUOTED, "\"ac\""), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT u\"ac\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "u"), Pair.of(DQUOTED, "\"ac\""), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT U\"ac\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "U"), Pair.of(DQUOTED, "\"ac\""), SQL_SPACE, SQL_FROM)));

    //
    // PGSQL-specific on PGSQL

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT u&\"ac\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DQUOTED, "u&\"ac\""), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT U&\"ac\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DQUOTED, "U&\"ac\""), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT u\"ac\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "u"), Pair.of(DQUOTED, "\"ac\""), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT U\"ac\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "U"), Pair.of(DQUOTED, "\"ac\""), SQL_SPACE, SQL_FROM)));

    //
    // PGSQL-specific on PGSQL parsing edge cases

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT u"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "u"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT U&"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "U"), Pair.of(OPERATOR, "&"))));

    //
    // newlines inside of double-quoted strings
    // note: newlines are allowed inside double-quoted string literals (PGSQL, ORACLE)

    assertThat(
      runTokenizeSqlQuery(null, "SELECT \"newlines\nare\rallowed\" FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DQUOTED, "\"newlines\nare\rallowed\""), SQL_SPACE, SQL_FROM)));

  }


  @Test
  public void testTokenizeMultiLineComments() {
    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment % */ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment % */"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT /*+ hint(#$^&\\) */ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/*+ hint(#$^&\\) */"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment \n\r\n\r */ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment \n\r\n\r */"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment % */ FROM /* comment ! */ "),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment % */"), SQL_SPACE, SQL_FROM, SQL_SPACE, Pair.of(MCOMMENT, "/* comment ! */"), SQL_SPACE)));

    //
    // parsing edge cases

    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment \n\r\n\r */"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment \n\r\n\r */"))));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment \n\r\n\r *"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "comment"), Pair.of(WHITESPACE, " \n\r\n\r "), Pair.of(OPERATOR, "*"))));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment \n\r\n\r "),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "comment"), Pair.of(WHITESPACE, " \n\r\n\r "))));

    //
    // nested comments

    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment /* nesting */ test */"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment /* nesting */"), SQL_SPACE, Pair.of(IDENTIFIER, "test"), SQL_SPACE, Pair.of(OPERATOR, "*"), Pair.of(OPERATOR, "/"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT /* comment /* nesting */ oracle */"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment /* nesting */"), SQL_SPACE, Pair.of(IDENTIFIER, "oracle"), SQL_SPACE, Pair.of(OPERATOR, "*"), Pair.of(OPERATOR, "/"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT /* comment /* nesting */ postgres */"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment /* nesting */ postgres */"))));

    //
    // nested comments edge cases

    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment /* nesting */ test *"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment /* nesting */"), SQL_SPACE, Pair.of(IDENTIFIER, "test"), SQL_SPACE, Pair.of(OPERATOR, "*"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT /* comment /* nesting */ oracle *"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(MCOMMENT, "/* comment /* nesting */"), SQL_SPACE, Pair.of(IDENTIFIER, "oracle"), SQL_SPACE, Pair.of(OPERATOR, "*"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT /* comment /* nesting */ postgres *"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL,"/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "comment"), SQL_SPACE, Pair.of(MCOMMENT, "/* nesting */"), SQL_SPACE, Pair.of(IDENTIFIER, "postgres"), SQL_SPACE, Pair.of(OPERATOR, "*"))));


    assertThat(
      runTokenizeSqlQuery(null, "SELECT /* comment /* nesting * test *"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL,"/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "comment"), SQL_SPACE,
        Pair.of(ILLEGAL,"/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "nesting"), SQL_SPACE,
        Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "test"), SQL_SPACE, Pair.of(OPERATOR, "*"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT /* comment /* nesting * oracle *"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL,"/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "comment"), SQL_SPACE,
        Pair.of(ILLEGAL,"/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "nesting"), SQL_SPACE,
        Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "oracle"), SQL_SPACE, Pair.of(OPERATOR, "*"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT /* comment /* nesting * postgres *"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL,"/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "comment"), SQL_SPACE,
        Pair.of(ILLEGAL,"/"), Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "nesting"), SQL_SPACE,
        Pair.of(OPERATOR, "*"), SQL_SPACE, Pair.of(IDENTIFIER, "postgres"), SQL_SPACE, Pair.of(OPERATOR, "*"))));

  }


  @Test
  public void testTokenizeSingleLineComments() {
    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \n *\r\n-- comment !\r FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % \n"),
        SQL_SPACE, SQL_STAR, SQL_CRNL, Pair.of(SCOMMENT, "-- comment !\r"),
        SQL_SPACE, SQL_FROM)));

    //
    // parsing ambiguous newlines

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \u000B FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % "),
        Pair.of(ILLEGAL, "\u000B"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \f FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % "),
        Pair.of(ILLEGAL, "\u000C"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \u0085 FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % "),
        Pair.of(ILLEGAL, "\u0085"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \u2028 FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % "),
        Pair.of(ILLEGAL, "\u2028"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \u2029 FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % "),
        Pair.of(ILLEGAL, "\u2029"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \r\f\tFROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % \r"),
        Pair.of(WHITESPACE, "\f\t"), // we have no problem with \f outside of single line comments
        SQL_FROM)));

    //
    // parsing edge cases

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \r-- comment % \r"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % \r"), Pair.of(SCOMMENT, "-- comment % \r"))));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \n-- comment % \n"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % \n"), Pair.of(SCOMMENT, "-- comment % \n"))));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % \r\n-- comment % \r\n"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % \r\n"), Pair.of(SCOMMENT, "-- comment % \r\n")
        )));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT -- comment % '"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(SCOMMENT, "-- comment % '"))));
  }


  @Test
  public void testTokenizePostgresDollarQuotes() {

    //
    // PGSQL-specific without PGSQL

    assertThat(
      runTokenizeSqlQuery(null, "SELECT $$dollar-quoted$$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE,
        Pair.of(ILLEGAL, "$"), Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "dollar"), Pair.of(OPERATOR, "-"), Pair.of(IDENTIFIER, "quoted$$"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(null, "SELECT $tag$dollar-quoted$tag$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE,
        Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "tag$dollar"), Pair.of(OPERATOR, "-"), Pair.of(IDENTIFIER, "quoted$tag$"),
        SQL_SPACE, SQL_FROM)));

    //
    // PGSQL-specific on PGSQL

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $$dollar-quoted$$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DSTRING, "$$dollar-quoted$$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $tag$dollar\nquoted$tag$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DSTRING, "$tag$dollar\nquoted$tag$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $_$dollar'quoted$_$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DSTRING, "$_$dollar'quoted$_$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $t1g$dollar+quoted$t1g$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DSTRING, "$t1g$dollar+quoted$t1g$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $$$$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DSTRING, "$$$$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $tag$$tag$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(DSTRING, "$tag$$tag$"), SQL_SPACE, SQL_FROM)));

    //
    // PGSQL-specific on PGSQL parsing edge cases

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $3ag$dollar-quoted$3ag$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE,
        Pair.of(ILLEGAL, "$"), Pair.of(NUMBER, "3"), Pair.of(IDENTIFIER, "ag$dollar"), Pair.of(OPERATOR, "-"), Pair.of(IDENTIFIER, "quoted$3ag$"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $-$dollar-quoted$-$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE,
        Pair.of(ILLEGAL, "$"), Pair.of(OPERATOR, "-"), Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "dollar"), Pair.of(OPERATOR, "-"),
        Pair.of(IDENTIFIER, "quoted$"), Pair.of(OPERATOR, "-"), Pair.of(ILLEGAL, "$"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $t g$dollar-quoted$t g$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE,
        Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "t"), SQL_SPACE, Pair.of(IDENTIFIER, "g$dollar"),Pair.of(OPERATOR, "-"),
        Pair.of(IDENTIFIER, "quoted$t"), SQL_SPACE, Pair.of(IDENTIFIER, "g$"),
        SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT x$_$dollar-quoted$_$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "x$_$dollar"), Pair.of(OPERATOR, "-"), Pair.of(IDENTIFIER, "quoted$_$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $tag$tag$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "tag$tag$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $$dollar-quoted$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "dollar"),Pair.of(OPERATOR, "-"),
        Pair.of(IDENTIFIER, "quoted$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $$dollar-quoted FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "dollar"),Pair.of(OPERATOR, "-"),
        Pair.of(IDENTIFIER, "quoted"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $tag$dollar-quoted$tag FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "tag$dollar"),Pair.of(OPERATOR, "-"),
        Pair.of(IDENTIFIER, "quoted$tag"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $tag$dollar-quoted$ FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "tag$dollar"),Pair.of(OPERATOR, "-"),
        Pair.of(IDENTIFIER, "quoted$"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $tag$dollar-quoted FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "tag$dollar"),Pair.of(OPERATOR, "-"),
        Pair.of(IDENTIFIER, "quoted"), SQL_SPACE, SQL_FROM)));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $tag$"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "tag$"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $tag"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(IDENTIFIER, "tag"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.PGSQL, "SELECT $$"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(ILLEGAL, "$"), Pair.of(ILLEGAL, "$"))));
  }


  @Test
  public void testTokenizeOracleAlternativeQuotingMechanism() {

    //
    // ORACLE-specific without ORACLE

    assertThat(
      runTokenizeSqlQuery(null, "SELECT q'!name LIKE '%0'!' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE,
        Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'!name LIKE '"), Pair.of(OPERATOR, "%"), Pair.of(NUMBER, "0"), Pair.of(STRING, "'!'"),
        SQL_SPACE, SQL_FROM)));

    //
    // ORACLE-specific on ORACLE

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'!name LIKE '%0'!' FROM"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'!name LIKE '%0'!'"), SQL_SPACE, SQL_FROM)));

    //
    // ORACLE-specific on ORACLE parsing edge cases

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT nq"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "nq"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT qn"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "qn"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'!name!'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'!name!'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'!na!me!'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'!na!me!'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'[name]'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'[name]'"))));
    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'(name)'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'(name)'"))));
    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'{name}'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'{name}'"))));
    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'<name>'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'<name>'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'<naq'(name)'me>'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'<naq'(name)'me>'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q''name''"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q''name''"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q''na'me''"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q''na'me''"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'!name?'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'!name?'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'!na'me?'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'!na'"), Pair.of(IDENTIFIER, "me"), Pair.of(OPERATOR, "?"), Pair.of(ILLEGAL, "'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q' na'me '"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "' na'"), Pair.of(IDENTIFIER, "me"), Pair.of(WHITESPACE, " "), Pair.of(ILLEGAL, "'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'\tna'me\t'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'\tna'"), Pair.of(IDENTIFIER, "me"), Pair.of(WHITESPACE, "\t"), Pair.of(ILLEGAL, "'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'\rna'me\r'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'\rna'"), Pair.of(IDENTIFIER, "me"), Pair.of(WHITESPACE, "\r"), Pair.of(ILLEGAL, "'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'\fna'me\f'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'\fna'"), Pair.of(IDENTIFIER, "me"), Pair.of(WHITESPACE, "\f"), Pair.of(ILLEGAL, "'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'[name['"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'[name['"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'(name('"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'(name('"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'{name{'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'{name{'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'<name<'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(STRING, "'<name<'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q']name]'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q']name]'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q')name)'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q')name)'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'}name}'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'}name}'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'>name>'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(STRING, "q'>name>'"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'!name!"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(ILLEGAL, "'"), Pair.of(OPERATOR, "!"), Pair.of(IDENTIFIER, "name"), Pair.of(OPERATOR, "!"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'!name"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(ILLEGAL, "'"), Pair.of(OPERATOR, "!"), Pair.of(IDENTIFIER, "name"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'!"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(ILLEGAL, "'"), Pair.of(OPERATOR, "!"))));

    assertThat(
      runTokenizeSqlQuery(SqlTokenizer.ORACLE, "SELECT q'"),
      equalTo(ImmutableList.of(
        SQL_SELECT, SQL_SPACE, Pair.of(IDENTIFIER, "q"), Pair.of(ILLEGAL, "'"))));
  }
}
