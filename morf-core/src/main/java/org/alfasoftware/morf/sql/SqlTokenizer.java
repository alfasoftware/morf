package org.alfasoftware.morf.sql;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * SQL parsing tokenizer implementation.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2024
 */
public final class SqlTokenizer {

  public static final String ORACLE = "ORACLE";
  public static final String PGSQL = "PGSQL";

  /**
   * Tokenization result.
   */
  public static final class Token {

    private final TokenType type;
    private final String tokenString;
    private final int position;

    Token(TokenType type, String query, int from, int to) {
      this.type = type;
      this.position = from;
      this.tokenString = query.substring(from, to);
    }

    public TokenType getType() {
      return type;
    }

    public String getString() {
      return tokenString;
    }

    public int getPosition() {
      return position;
    }
  }


  /**
   * Type of a token.
   */
  public enum TokenType {
    /**
     * Sequence of white-spaces, including newlines.
     */
    WHITESPACE,

    /**
     * Single identifier or a keyword.
     * Note that the tokenizer does not attempt semantic analysis,
     * therefore keywords are not being distinguished from identifiers.
     */
    IDENTIFIER,

    /**
     * Single number.
     */
    NUMBER,

    /**
     * Double-quoted identifier, including the double-quotes, and any leading classifiers.
     */
    DQUOTED,

    /**
     * Single-quoted string literal, including the single-quotes.
     */
    STRING,

    /**
     * Dollar-quoted string literal, including the dollar-quotes.
     */
    DSTRING,

    /**
     * Single-character operator.
     */
    OPERATOR,

    /**
     * Single-line comment, including the comment-starting characters and comment-ending newline.
     */
    SCOMMENT,

    /**
     * Multi-line comment, including the comment-starting characters and comment-ending characters.
     */
    MCOMMENT,

    /**
     * An illegal or unexpected character character, exactly as found.
     */
    ILLEGAL
  }


  /**
   * Pure static class.
   */
  private SqlTokenizer() {
  }


  /**
   * Tokenizes given query, i.e. splits the query into individual tokens. See {@link TokenType}.
   *
   * <p>
   * The caller can examine the tokens via the provided callback function, which is called once for each token.
   * The function can, for example, be used to collect all the tokens into a list, or to validate the tokens.
   * </p>
   *
   * <p>
   * Note: This tokenizer reports white-spaces and comments as tokens, even though they are not really tokens.
   * </p>
   *
   * <p>
   * All tokens, simply concatenated back together, will always give the original input query,
   * i.e. no characters are added/removed/translated, no tokens are ever excluded, or reordered.
   * </p>
   *
   * <p>Simple example:</p>
   * <pre>SELECT * FROM table</pre>
   * <p>Is tokenized as:</p>
   * <pre>IDENTIFIER(SELECT), WHITESPACE( ), OPERATOR(*), WHITESPACE( ), IDENTIFIER(FROM), WHITESPACE( ), IDENTIFIER(table)</pre>
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param visitor Function which will receive TokenType+String pairs.
   *                The string contains the entire token, as described by the respective TokenType.
   */
  public static void tokenizeSqlQuery(String databaseType, String query, Consumer<Token> visitor) {
    final int length = query.length();
    int i = 0;

    while (i < length) {

      // single-quoted strings
      int sqs = findSingleQuotedString(databaseType, query, length, i);
      if (sqs > 0) {
        visitor.accept(new Token(TokenType.STRING, query, i, sqs));
        i = sqs;
        continue;
      }

      // dollar-quoted strings
      int dqs = findDollarQuotedString(databaseType, query, length, i);
      if (dqs > 0) {
        visitor.accept(new Token(TokenType.DSTRING, query, i, dqs));
        i = dqs;
        continue;
      }

      // double-quoted identifiers
      int dqi = findDoubleQuotedIdentifier(databaseType, query, length, i);
      if (dqi > 0) {
        visitor.accept(new Token(TokenType.DQUOTED, query, i, dqi));
        i = dqi;
        continue;
      }

      // keywords/identifiers/etc.
      int kid = findKeywordIdentifier(databaseType, query, length, i);
      if (kid > 0) {
        visitor.accept(new Token(TokenType.IDENTIFIER, query, i, kid));
        i = kid;
        continue;
      }

      // numbers
      int num = findNumber(databaseType, query, length, i);
      if (num > 0) {
        visitor.accept(new Token(TokenType.NUMBER, query, i, num));
        i = num;
        continue;
      }

      // white-spaces
      int wsp = findWhitespace(databaseType, query, length, i);
      if (wsp > 0) {
        visitor.accept(new Token(TokenType.WHITESPACE, query, i, wsp));
        i = wsp;
        continue;
      }

      // single-line comments
      AtomicBoolean commentCutShort = new AtomicBoolean(false);
      int slc = matchSingleLineComment(databaseType, query, length, i, commentCutShort);
      if (slc > 0) {
        visitor.accept(new Token(TokenType.SCOMMENT, query, i, slc));
        i = slc;
        // make sure we react to ambiguous newlines
        if (commentCutShort.get()) {
          if (i < length && isAmbiguousNewlineCharacter(query.charAt(i))) {
            visitor.accept(new Token(TokenType.ILLEGAL, query, i, i + 1));
            i++;
          }
        }
        continue;
      }

      // multi-line comments
      AtomicBoolean commentUnfinished = new AtomicBoolean(false);
      int mlc = matchMultiLineComment(databaseType, query, length, i, commentUnfinished);
      if (mlc > 0) {
        visitor.accept(new Token(TokenType.MCOMMENT, query, i, mlc));
        i = mlc;
        continue;
      }
      // make sure we react to unfinished comments
      else if (commentUnfinished.get()) {
        if (i < length) {
          visitor.accept(new Token(TokenType.ILLEGAL, query, i, i + 1));
          i++;
        }
      }

      // everything supported as a single-character operator token
      if (isSupportedOpCharacter(query.charAt(i))){
        visitor.accept(new Token(TokenType.OPERATOR, query, i, i + 1));
        i++;
        continue;
      }

      // everything else as a single-character illegal token
      visitor.accept(new Token(TokenType.ILLEGAL, query, i, i + 1));
      i++;
      continue;
    }
  }


  /**
   * Tries to match a single-quoted string literal.
   *
   * See: https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS
   * See: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Literals.html
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param length Total length of the query.
   * @param i Position where this token matching should start.
   * @return End position of the token, if matched at this position, or negative number otherwise.
   */
  private static int findSingleQuotedString(String databaseType, String query, int length, int i) {
    boolean allowBackslashEscapes = false;
    boolean oracleAlternativeQuotingMechanism = false;
    char oracleAlternativeQuotingChar;

    // Oracle UNICODE
    if (ORACLE.equals(databaseType)) {
      if (i < length && (query.charAt(i) == 'n' || query.charAt(i) == 'N')) {
        i++;
      }
      if (i < length && (query.charAt(i) == 'q' || query.charAt(i) == 'Q')) {
        oracleAlternativeQuotingMechanism = true;
        i++;
      }
    }
    else if (PGSQL.equals(databaseType)) {
      // Postgres BINARY/HEX
      if (i < length && (query.charAt(i) == 'b' || query.charAt(i) == 'B' || query.charAt(i) == 'x' || query.charAt(i) == 'X')) {
        i++;
      }
      // Postgres UNICODE
      else if (i < length && (query.charAt(i) == 'u' || query.charAt(i) == 'U') && i+1 < length && query.charAt(i+1) == '&') {
        i+=2;
      }
      // Postgres C-Style Escapes
      else if (i < length && (query.charAt(i) == 'e' || query.charAt(i) == 'E')) {
        allowBackslashEscapes = true;
        i++;
      }
    }

    if (i < length && query.charAt(i) == '\'') {
      i++;

      if (oracleAlternativeQuotingMechanism) {
        if (i < length) {
          oracleAlternativeQuotingChar = query.charAt(i);
          i++;

          if (Character.isWhitespace(oracleAlternativeQuotingChar))
            return -1;

          if (oracleAlternativeQuotingChar == '[')
            oracleAlternativeQuotingChar = ']';
          if (oracleAlternativeQuotingChar == '(')
            oracleAlternativeQuotingChar = ')';
          if (oracleAlternativeQuotingChar == '{')
            oracleAlternativeQuotingChar = '}';
          if (oracleAlternativeQuotingChar == '<')
            oracleAlternativeQuotingChar = '>';

          while (i < length) {
            char c = query.charAt(i);
            i++;

            if (c == oracleAlternativeQuotingChar && i < length && query.charAt(i) == '\'')
              return i + 1;
          }
          return -1;
        }
        return -1;
      }

      while (i < length) {
        char c = query.charAt(i);
        i++;

        if (c == '\'') {
          if (i < length && query.charAt(i) == '\'')
            i++; // two quotes - continue the string
          else
            return i;
        }

        if (allowBackslashEscapes)
          if (c == '\\' && i < length && query.charAt(i) == '\'')
            i++; // escaped quote - continue the string
      }

      return -1;
    }

    return -1;
  }


  /**
   * Tries to match a dollar-quoted string.
   *
   * See: https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param length Total length of the query.
   * @param i Position where this token matching should start.
   * @return End position of the token, if matched at this position, or negative number otherwise.
   */
  private static int findDollarQuotedString(String databaseType, String query, int length, int i) {
    // $-quoted strings
    if (PGSQL.equals(databaseType)) {
      if (query.charAt(i) == '$') {
        int start = i;
        i++;

        if (i < length && isIdentifierStartCharacter(query.charAt(i))) {
          i++;

          while (i < length) {
            if (query.charAt(i) == '$')
              break;
            if (!isIdentifierMidCharacter(query.charAt(i)))
                return -1;
            i++;
          }
        }

        if (i >= length)
          return -1;
        if (query.charAt(i) != '$')
          return -1;

        final String tag = query.substring(start, i + 1);
        final int end = query.indexOf(tag, i + 1);

        if (end >= 0)
          return end + tag.length();

        return -1;
      }
    }

    return -1;
  }


  /**
   * Tries to match a double-quoted identifier.
   *
   * See: https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
   * See: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param length Total length of the query.
   * @param i Position where this token matching should start.
   * @return End position of the token, if matched at this position, or negative number otherwise.
   */
  private static int findDoubleQuotedIdentifier(String databaseType, String query, int length, int i) {
    // Oracle UNICODE
    if (ORACLE.equals(databaseType)) {

    }
    else if (PGSQL.equals(databaseType)) {
      if ((query.charAt(i) == 'u' || query.charAt(i) == 'U') && i+1 < length && query.charAt(i+1) == '&' && i+2 < length && query.charAt(i+2) == '"') {
        i+=2;
      }
    }

    if (query.charAt(i) == '"') {
      i++;
      while (i < length) {
        char c = query.charAt(i);
        i++;

        if (c == '"') {
          if (i < length && query.charAt(i) == '"')
            i++; // two quotes - continue the string
          else
            return i;
        }
      }

      return -1;
    }

    return -1;
  }


  /**
   * Tries to match a keyword or an identifier.
   *
   * See https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
   * See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param length Total length of the query.
   * @param i Position where this token matching should start.
   * @return End position of the token, if matched at this position, or negative number otherwise.
   */
  private static int findKeywordIdentifier(@SuppressWarnings("unused") String databaseType, String query, int length, int i) {
    if (isIdentifierStartCharacter(query.charAt(i))) {
      i++;

      while (i < length) {
        if (!isIdentifierMidCharacter(query.charAt(i)))
            break;
        i++;
      }

      return i;
    }

    return -1;
  }


  /**
   * Tries to match a number.
   *
   * Note: Numbers currently do not include the decimal point, i.e. "12.98" is tokenized as "12" and "." and "98".
   * Note: Numbers currently do not include the scientific notation, i.e. "1.925e-3" is tokenized as "1" and "." and "925" and "e" and "-" and "3".
   * Note: Numbers starting with decimal point do not include the decimal point, i.e. ".98" is tokenized as "." and "98".
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param length Total length of the query.
   * @param i Position where this token matching should start.
   * @return End position of the token, if matched at this position, or negative number otherwise.
   */
  private static int findNumber(@SuppressWarnings("unused") String databaseType, String query, int length, int i) {
    if (isNumberStartCharacter(query.charAt(i))) {
      i++;

      while (i < length) {
        if (!isNumberMidCharacter(query.charAt(i)))
            break;
        i++;
      }

      return i;
    }

    return -1;
  }


  /**
   * Tries to match a sequence of white-spaces.
   *
   * Note: White-spaces include tabs and newlines.
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param length Total length of the query.
   * @param i Position where this token matching should start.
   * @return End position of the token, if matched at this position, or negative number otherwise.
   */
  private static int findWhitespace(@SuppressWarnings("unused") String databaseType, String query, int length, int i) {
    if (Character.isWhitespace(query.charAt(i))) {
      i++;

      while (i < length) {
        if (!Character.isWhitespace(query.charAt(i)))
            break;
        i++;
      }

      return i;
    }

    return -1;
  }


  /**
   * Tries to match a single-line comment.
   *
   * See https://www.postgresql.org/docs/8.0/sql-syntax.html#SQL-SYNTAX-COMMENTS
   * See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Comments.html
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param length Total length of the query.
   * @param i Position where this token matching should start.
   * @param commentCutShort This is set to true, when ambiguous newline characters are found ending the comment.
   * @return End position of the token, if matched at this position, or negative number otherwise.
   */
  private static int matchSingleLineComment(@SuppressWarnings("unused") String databaseType, String query, int length, int i, AtomicBoolean commentCutShort) {
    if (query.charAt(i) == '-' && i+1 < length && query.charAt(i+1) == '-') {
      while (i < length) {
        char c = query.charAt(i);

        // end of the single-line comment
        if (c == '\n')
          return i + 1;
        if (c == '\r' && i+1 < length && query.charAt(i+1) == '\n')
          return i + 2;
        if (c == '\r')
          return i + 1;

        if (isAmbiguousNewlineCharacter(c)) {
          commentCutShort.set(true);
          return i; // stop the comment right here
        }

        i++;
      }

      return i;
    }

    return -1;
  }


  /**
   * Tries to match a multi-line comment.
   *
   * Note: This currently matches hints, since they are effectively multi-line comments.
   *
   * See https://www.postgresql.org/docs/8.0/sql-syntax.html#SQL-SYNTAX-COMMENTS
   * See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Comments.html
   *
   * @param databaseType ORACLE/PGSQL
   * @param query Query to be tokenized.
   * @param length Total length of the query.
   * @param i Position where this token matching should start.
   * @param commentUnfinished This is set to true, when the comment is left unfinished before the query ends.
   * @return End position of the token, if matched at this position, or negative number otherwise.
   */
  private static int matchMultiLineComment(String databaseType, String query, int length, int i, AtomicBoolean commentUnfinished) {
    boolean allowCommentNesting = false;

    if (PGSQL.equals(databaseType)) {
      allowCommentNesting = true;
    }

    if (query.charAt(i) == '/' && i+1 < length && query.charAt(i+1) == '*') {
      i += 2;
      while (i < length) {
        // end of the multi-line comment
        if (query.charAt(i) == '*' && i+1 < length && query.charAt(i+1) == '/')
          return i + 2;

        if (allowCommentNesting) {
          if (query.charAt(i) == '/' && i+1 < length && query.charAt(i+1) == '*') {
            int mlc = matchMultiLineComment(databaseType, query, length, i, commentUnfinished);
            if (mlc > 0)
              i = mlc;
          }
        }

        i++;
      }

      commentUnfinished.set(true);
      return -1;
    }

    return -1;
  }


  /**
   * See https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
   * See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html
   */
  private static boolean isIdentifierStartCharacter(char c) {
    return 'A' <= c && c <= 'Z'
        || 'a' <= c && c <= 'z'
        || c == '_';
  }


  /**
   * See https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
   * See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html
   */
  private static boolean isIdentifierMidCharacter(char c) {
    return isIdentifierStartCharacter(c)
        || '0' <= c && c <= '9'
      //|| c == '#' // Oracle allows # in identifiers; but we do not currently support it
        || c == '$';
  }


  /**
   * See https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC
   * See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Literals.html
   */
  private static boolean isNumberStartCharacter(char c) {
    return '0' <= c && c <= '9';
  }


  /**
   * See https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC
   * See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Literals.html
   */
  private static boolean isNumberMidCharacter(char c) {
    return isNumberStartCharacter(c);
  }


  /**
   * Recognises all operator characters.
   * Characters not recognised by this method and not matched as part of other tokens are considered illegal.
   *
   * @param c Character to be recognised.
   * @return True, if the character is a valid SQL character.
   */
  private static boolean isSupportedOpCharacter(char c) {
    switch (c) {
      case ',': case '.': // identifier separators
      case '(': case ')': // bracketing
      case '!': case '=': case '>': case '<': // maths
      case '+': case '-': case '*': case '/': // maths
      case '|': case '&':  case '#': // bitwise operations and oracle concat
      case '%': case '^': // maths
      case '~': // used for regexps
      case ':': case '?': // parameters
      case ';': // end of statement
      case '@': // used for db links and maths
        return true;

      case '[': case ']': // bracketing
      case '{': case '}': // bracketing
        // do not allow unnecessary brackets
        return false;

      case '\\': // important: careful! should not be needed, hence it is illegal
        return false;

      case '\'': // important: should be handled by single-quoted string; otherwise it is illegal
      case '"': // important: should be handled by double-quoted string; otherwise it is illegal
      case '`': // important: should be handled by backtick-quoted string; otherwise it is illegal
      case '_': // important: should be handled by identifiers; otherwise it is illegal
      case '$': // important: should be handled by identifiers; otherwise it is illegal
        return false;

      default:
        // do not allow unnecessary characters
        // these might have special meaning in different dialects
        // note: these can be handled/allowed within single-quoted string literals
        return false;
    }
  }


  /**
   * These characters can potentially be treated as newlines by some vendors,
   * but they also equally might be not treated as newlines by other vendors.
   * Therefore, we shall strictly forbid their use in newline-important situations,
   * to avoid ambiguity around how comments and the rest of the query will be parsed.
   * This is to avoid any risk of "SQL injection" based on the difference in parsing.
   */
  private static boolean isAmbiguousNewlineCharacter(char c) {
    if (c == '\u000B') return true; // VT: Vertical Tab
    if (c == '\u000C') return true; // FF: Form Feed
    if (c == '\u0085') return true; // NEL: Next Line
    if (c == '\u2028') return true; // LS: Line Separator
    if (c == '\u2029') return true; // PS: Paragraph Separator

    return false;
  }
}
