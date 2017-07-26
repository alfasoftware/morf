/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.metadata.DataType.STRING;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.sql.element.Criterion.eq;

import java.util.List;

import org.joda.time.LocalDate;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.BracketedExpression;
import org.alfasoftware.morf.sql.element.CaseStatement;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.MathsField;
import org.alfasoftware.morf.sql.element.MathsOperator;
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.sql.element.WhenCondition;
import org.alfasoftware.morf.sql.element.WindowFunction;

/**
 * Utility methods for creating SQL constructs.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class SqlUtils {


  /**
   * Constructs an update statement.
   *
   * @see UpdateStatement#UpdateStatement(TableReference)
   * @param tableReference the database table to update
   * @return {@link UpdateStatement}
   */
  public static UpdateStatement update(TableReference tableReference) {
    return new UpdateStatement(tableReference);
  }


  /**
   * Construct a new table with a given name.
   *
   * @param tableName the name of the table
   * @return {@link TableReference}
   */
  public static TableReference tableRef(String tableName) {
    return new TableReference(tableName);
  }


  /**
   * Constructs a Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all
   * fields (i.e. {@code SELECT * FROM x}).
   *
   * @param fields an array of fields that should be selected
   * @return {@link SelectStatement}
   */
  public static SelectStatement select(AliasedFieldBuilder... fields) {
    return new SelectStatement(fields);
  }


  /**
   * Constructs a Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all
   * fields (i.e. {@code SELECT * FROM x}).
   *
   * @param fields fields that should be selected
   * @return {@link SelectStatement}
   */
  public static SelectStatement select(List<? extends AliasedFieldBuilder> fields) {
    return new SelectStatement(fields);
  }


  /**
   * Constructs a distinct Select Statement which optionally selects on a subset of fields.
   * If no fields are specified then this is equivalent of selecting all
   * fields (i.e. {@code SELECT DISTINCT * FROM x}).
   *
   * @param fields an array of fields that should be selected
   * @return {@link SelectStatement}
   */
  public static SelectStatement selectDistinct(AliasedFieldBuilder... fields) {
    return new SelectStatement(fields, true);
  }


  /**
   * Constructs a Select First Statement
   *
   * @param field the field that should be selected
   * @return {@link SelectStatement}
   */
  public static SelectFirstStatement selectFirst(AliasedFieldBuilder field) {
    return new SelectFirstStatement(field);
  }


  /**
   * Constructs an Insert Statement.
   *
   * @return {@link InsertStatement}
   */
  public static InsertStatement insert() {
    return new InsertStatement();
  }


  /**
   * Constructs a Delete Statement.
   *
   * @param table the database table to delete from.
   * @return {@link DeleteStatement}
   */
  public static DeleteStatement delete(TableReference table) {
    return new DeleteStatement(table);
  }


  /**
   * Constructs a Merge Statement which either inserts or updates
   * a record into a table depending on whether a condition exists in
   * the table.
   *
   * @return {@link MergeStatement}
   */
  public static MergeStatement merge() {
    return new MergeStatement();
  }


  /**
   * Constructs a Truncate Statement.
   *
   * @param table The table to truncate.
   * @return The statement.
   */
  public static TruncateStatement truncate(TableReference table) {
    return new TruncateStatement(table);
  }


  /**
   * Method that wraps a first elements of an (sub)expression with a bracket.
   * <p>
   * For example, in order to generate "(a + b) / c" SQL Math expression, we
   * need to put first two elements (first subexpression) into a bracket. That
   * could be achieved by the following DSL statement.
   * </p>
   *
   * <pre>
   * bracket(field(&quot;a&quot;).plus(field(&quot;b&quot;))).divideBy(field(&quot;c&quot;))
   * </pre>
   *
   *
   * @param expression the input Math expression that will be wrapped with
   *          brackets in output SQL
   * @return new expression containing the input expression wrapped with
   *         brackets
   */
  public static AliasedField bracket(MathsField expression) {
    return new BracketedExpression(expression);
  }


  /**
   * Constructs a new field with a given name
   *
   * <p>Consider using {@link TableReference#field(String)} instead.</p>
   *
   * @param fieldName the name of the field
   * @return {@link FieldReference}
   * @see TableReference#field(String)
   */
  public static FieldReference field(String fieldName) {
    return new FieldReference(fieldName);
  }


  /**
   * Constructs a new {@link FieldLiteral} with a string source.
   *
   * @param value the literal value to use
   * @return {@link FieldLiteral}
   */
  public static FieldLiteral literal(String value) {
    return new FieldLiteral(value);
  }


  /**
   * Constructs a new {@link FieldLiteral} with a {@link LocalDate} source.
   *
   * @param value the literal value to use
   * @return {@link FieldLiteral}
   */
  public static FieldLiteral literal(LocalDate value) {
    return new FieldLiteral(value);
  }


  /**
   * Constructs a new {@link FieldLiteral} with an Integer source.
   *
   * @param value the literal value to use
   * @return {@link FieldLiteral}
   */
  public static FieldLiteral literal(int value) {
    return new FieldLiteral(value);
  }


  /**
   * Constructs a new {@link FieldLiteral} with a Long source.
   *
   * @param value the literal value to use
   * @return {@link FieldLiteral}
   */
  public static FieldLiteral literal(long value) {
    return new FieldLiteral(value);
  }


  /**
   * Constructs a new {@link FieldLiteral} with a double source.
   *
   * @param value the literal value to use
   * @return {@link FieldLiteral}
   */
  public static FieldLiteral literal(double value) {
    return new FieldLiteral(value);
  }


  /**
   * Constructs a new {@link FieldLiteral} with a boolean source.
   *
   * @param value the literal value to use
   * @return {@link FieldLiteral}
   */
  public static FieldLiteral literal(boolean value) {
    return new FieldLiteral(value);
  }


  /**
   * Constructs a new {@link FieldLiteral} with a Character source.
   *
   * @param value the literal value to use
   * @return {@link FieldLiteral}
   */
  public static FieldLiteral literal(Character value) {
    return new FieldLiteral(value);
  }


  /**
   * Constructs a new SQL named parameter.  Usage:
   *
   * <pre>parameter("name").type(DataType.INTEGER)
parameter("name").type(DataType.STRING).width(10)
parameter("name").type(DataType.DECIMAL).width(13,2)</pre>
   *
   * @param name the parameter name.
   * @return {@link SqlParameter}
   */
  public static SqlParameterBuilder parameter(String name) {
    return new SqlParameterBuilder(name);
  }


  /**
   * Constructs a new SQL named parameter from a column.
   *
   * @param column the parameter column.
   * @return {@link SqlParameter}
   */
  public static SqlParameter parameter(Column column) {
    return new SqlParameter(column);
  }


  /**
   * Shortcut to "not empty and not null"
   *
   * @param expression the expression to evaluate.
   * @return an expression wrapping the passed expression with additional
   * criteria to ensure it is not blank or null.
   */
  public static Criterion isNotEmpty(AliasedField expression) {
    return Criterion.and(
      Criterion.neq(expression, literal("")),
      Criterion.isNotNull(expression)
    );
  }


  /**
   * Builder method for {@link WhenCondition}.
   *
   * <p>
   * Example:
   * </p>
   *
   * <pre>
   * <code>
   *   caseStatement(when(eq(field("receiptType"), literal("R"))).then(literal("Receipt")),
   *                 when(eq(field("receiptType"), literal("S"))).then(literal("Agreement Suspense")),
   *                 when(eq(field("receiptType"), literal("T"))).then(literal("General Suspense")))
   *                .otherwise(literal("UNKNOWN"))
   * </code>
   * </pre>
   *
   * @see #caseStatement(WhenCondition...)
   *
   * @param criterion Criteria
   * @return A builder to create a {@link WhenCondition}.
   */
  public static WhenConditionBuilder when(Criterion criterion) {
    return new WhenConditionBuilder(criterion);
  }


  /**
   * Builder method for {@link CaseStatement}.
   *
   * <p>
   * Example:
   * </p>
   *
   * <pre>
   * caseStatement(when(eq(field(&quot;receiptType&quot;), literal(&quot;R&quot;))).then(literal(&quot;Receipt&quot;)),
   *               when(eq(field(&quot;receiptType&quot;), literal(&quot;S&quot;))).then(literal(&quot;Agreement Suspense&quot;)),
   *               when(eq(field(&quot;receiptType&quot;), literal(&quot;T&quot;))).then(literal(&quot;General Suspense&quot;)))
   *              .otherwise(literal(&quot;UNKNOWN&quot;))
   * </pre>
   *
   * @see #when(Criterion)
   *
   * @param whenClauses the {@link WhenCondition} portions of the case statement
   * @return A builder to create a {@link CaseStatement}.
   */
  public static CaseStatementBuilder caseStatement(WhenCondition... whenClauses) {
    return new CaseStatementBuilder(whenClauses);
  }


  /**
   * @return a {@link NullFieldLiteral}.
   */
  public static FieldLiteral nullLiteral() {
    return new NullFieldLiteral();
  }


  /**
   * Returns null if the supplied expression is zero,
   * otherwise returns the expression.
   *
   * @param expression the expression to evaluate
   * @return null or the expression
   */
  public static AliasedField nullLiteralIfZero(AliasedField expression) {
    return caseStatement(
      when(eq(expression, literal(0))).then(nullLiteral())
    ).otherwise(expression);
  }


  /**
   * Returns null if field is zero, otherwise returns the expression.
   *
   * @param field expression to check for zero.
   * @param expression expression to return if field is not zero.
   * @return null or the expression
   */
  public static AliasedField nullLiteralIfFieldIsZero(AliasedField field, AliasedField expression) {
    return caseStatement(
      when(field.eq(0)).then(nullLiteral())
    ).otherwise(expression);
  }


  /**
   * @see CastBuilder#asString(int)
   * @param field the field to cast
   * @return A builder to produce a {@link Cast}.
   */
  public static CastBuilder cast(AliasedField field) {
    return new CastBuilder(field);
  }


  /**
   * Builder for {@link WhenCondition}.
   *
   * @see SqlUtils#when(Criterion)
   * @see #then(AliasedField)
   * @author Copyright (c) Alfa Financial Software 2012
   */
    public static final class WhenConditionBuilder {

    private final Criterion criterion;


    /**
     * @param criterion Criteria
     */
    private WhenConditionBuilder(Criterion criterion) {
      this.criterion = criterion;
    }


    /**
     * @param value The value returned if the criteria is true
     * @return {@link WhenCondition}
     */
    public WhenCondition then(AliasedField value) {
      return new WhenCondition(criterion, value);
    }


    /**
     * @param value The value returned if the criteria is true
     * @return {@link WhenCondition}
     */
    public WhenCondition then(String value) {
      return then(literal(value));
    }


    /**
     * @param value The value returned if the criteria is true
     * @return {@link WhenCondition}
     */
    public WhenCondition then(boolean value) {
      return then(literal(value));
    }


    /**
     * @param value The value returned if the criteria is true
     * @return {@link WhenCondition}
     */
    public WhenCondition then(int value) {
      return then(literal(value));
    }


    /**
     * @param value The value returned if the criteria is true
     * @return {@link WhenCondition}
     */
    public WhenCondition then(long value) {
      return then(literal(value));
    }
  }


  /**
   * Builder for {@link CaseStatement}.
   *
   * @see SqlUtils#caseStatement(WhenCondition...)
   * @see #otherwise(AliasedField)
   * @author Copyright (c) Alfa Financial Software 2012
   */
    public static final class CaseStatementBuilder {

    private final WhenCondition[] whenClauses;


    /**
     * @param whenClauses the {@link WhenCondition} portions of the case statement
     */
    private CaseStatementBuilder(WhenCondition... whenClauses) {
      this.whenClauses = whenClauses;
    }


    /**
     * @param defaultValue If all the when conditions fail return this default value
     * @return {@link CaseStatement}
     */
    public CaseStatement otherwise(AliasedField defaultValue) {
      return new CaseStatement(defaultValue, whenClauses);
    }


    /**
     * @param defaultValue If all the when conditions fail return this default value
     * @return {@link CaseStatement}
     */
    public CaseStatement otherwise(String defaultValue) {
      return otherwise(literal(defaultValue));
    }


    /**
     * @param defaultValue If all the when conditions fail return this default value
     * @return {@link CaseStatement}
     */
    public CaseStatement otherwise(int defaultValue) {
      return otherwise(literal(defaultValue));
    }


    /**
     * @param defaultValue If all the when conditions fail return this default value
     * @return {@link CaseStatement}
     */
    public CaseStatement otherwise(long defaultValue) {
      return otherwise(literal(defaultValue));
    }


    /**
     * @param defaultValue If all the when conditions fail return this default value
     * @return {@link CaseStatement}
     */
    public CaseStatement otherwise(boolean defaultValue) {
      return otherwise(literal(defaultValue));
    }
  }


  /**
   * Builder class for constructing a {@link Cast}
   *
   * @author Copyright (c) Alfa Financial Software 2012
   */
  public static class CastBuilder {

    private final AliasedField field;


    /**
     * @param field field to cast
     */
    public CastBuilder(AliasedField field) {
      this.field = field;
    }


    /**
     * Returns a SQL DSL expression to return the field CASTed to
     * a string of the specified length
     *
     * @param length length of the string cast
     * @return {@link Cast} as string of given length
     */
    public Cast asString(int length) {
      return asType(STRING, length);
    }


    /**
     * Returns a SQL DSL expression to return the field CASTed to
     * the specified type.
     *
     * @param type The target type.
     * @return The SQL DSL expression applying the cast function.
     */
    public Cast asType(DataType type) {
      return new Cast(field, type, 0);
    }


    /**
     * Returns a SQL DSL expression to return the field CASTed to
     * the specified type and length.
     *
     * @param type The target type.
     * @param length The target length.
     * @return The SQL DSL expression applying the cast function.
     */
    public Cast asType(DataType type, int length) {
      return new Cast(field, type, length, 0);
    }


    /**
     * Returns a SQL DSL expression to return the field CASTed to
     * the specified type, length and scale.
     *
     * @param type The target type.
     * @param length The target length.
     * @param scale The target scale.
     * @return The SQL DSL expression applying the cast function.
     */
    public Cast asType(DataType type, int length, int scale) {
      return new Cast(field, type, length, scale);
    }
  }


  /**
   * Returns an expression concatenating all the passed expressions.
   *
   * @param fields the expressions to concatenate.
   * @return the expression concatenating the passed expressions.
   */
  public static ConcatenatedField concat(AliasedField... fields) {
    return new ConcatenatedField(fields);
  }


  /**
   * Returns a SQL DSL expression to return the substring of <strong>field</strong>
   * of <strong>length</strong> characters, starting from <strong>from</strong>.
   *
   * @param field The source expression.
   * @param from The start character offset.
   * @param length The length of the substring.
   * @return The SQL DSL expression applying the substring function.
   */
  public static AliasedField substring(AliasedField field, int from, int length) {
    return Function.substring(field, literal(from), literal(length));
  }



  /**
   * Returns a mathematical expression.
   *
   * @param leftField The left field
   * @param operator The operator
   * @param rightField The right field
   * @return the expression.
   */
  public static MathsField math(AliasedField leftField, MathsOperator operator, AliasedField rightField) {
    return new MathsField(leftField, operator, rightField);
  }


  /**
   * Encapsulates the generation of an PARTITION BY SQL statement.
   *
   * <p>The call structure imitates the end SQL and is structured as follows:</p>
   *
   * <blockquote><pre>
   *   SqlUtils.windowFunction([function])                         = [function]
   *        |----&gt; .partitionBy([fields]...)                    = [function] OVER (PARTITION BY [fields])
   *                |----&gt; .orderBy([fields]...)                = [function] OVER (PARTITION BY [fields] ORDER BY [fields])
   *        |----&gt; .orderBy([fields]...)                        = [function] OVER (ORDER BY [fields])
   *  </pre></blockquote>
   *
   * Restrictions:
   * <ul>
   * <li>partitionBy(..) is optional: If not specified it treats all the rows of the result set as a single group.</li>
   * <li>orderBy(..) is optional. If not specified the entire partition will be used as the window frame. If specified a range between the first row and the current row of the window is used (i.e. RANGE UNBOUNDED PRECEDING AND CURRENT ROW for Oracle).</li>
   * <li>The default direction for fields in orderBy(..) is ASC.</li>
   * </ul>
   * @author Copyright (c) Alfa Financial Software 2017
   * @param function The function
   * @return The windowing function builder
   */
  public static WindowFunction.Builder windowFunction(Function function) {
    return WindowFunction.over(function);
  }


  /**
   * Builds SQL parameters.
   *
   * @author Copyright (c) Alfa Financial Software 2014
   */
  public static final class SqlParameterBuilder {
    private final String name;

    private SqlParameterBuilder(String name) {
      this.name = name;
    }

    /**
     * Specifies the data type for the parameter.
     *
     * @param dataType The data type
     * @return the next phase of the parameter builder.
     */
    public SqlParameterWidthBuilder type(DataType dataType) {
      return new SqlParameterWidthBuilder(name, dataType);
    }
  }


  /**
   * Builds SQL parameters.  Subclasses {@link SqlParameter} so you can
   * stop here if you're just creating (say) an integer parameter where
   * no width information is required.
   *
   * @author Copyright (c) Alfa Financial Software 2014
   */
  public static final class SqlParameterWidthBuilder extends SqlParameter {

    private SqlParameterWidthBuilder(String name, DataType type) {
      super(column(name, type));
    }

    /**
     * Specifies the width of the parameter and
     * returns the constructed parameter.
     *
     * @param width The width
     * @return the {@link SqlParameter}.
     */
    public SqlParameter width(int width) {
      return new SqlParameter(column(getMetadata().getName(), getMetadata().getType(), width));
    }

    /**
     * Specifies the width and scale of the parameter and
     * returns the constructed parameter.
     *
     * @param width The width
     * @param scale The scale
     * @return the {@link SqlParameter}.
     */
    public SqlParameter width(int width, int scale) {
      return new SqlParameter(column(getMetadata().getName(), getMetadata().getType(), width, scale));
    }
  }
}