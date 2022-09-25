package org.alfasoftware.morf.upgrade;

import static org.mockito.Mockito.verifyNoInteractions;

import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.MergeStatement.InputField;
import org.alfasoftware.morf.sql.MinusSetOperator;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UnionSetOperator;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.BracketedExpression;
import org.alfasoftware.morf.sql.element.CaseStatement;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.FieldFromSelectFirst;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.MathsField;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.WhenCondition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests of the default behavior of {@link SchemaAndDataChangeVisitor}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestSchemaAndDataChangeVisitor {

  @Mock
  private AddColumn addColumn;

  @Mock
  private AddTable addTable;

  @Mock
  private RemoveTable removeTable;

  @Mock
  private AddIndex addIndex;

  @Mock
  private ChangeColumn changeColumn;

  @Mock
  private RemoveColumn removeColumn;

  @Mock
  private RemoveIndex removeIndex;

  @Mock
  private ChangeIndex changeIndex;

  @Mock
  private RenameIndex renameIndex;

  @Mock
  private RenameTable renameTable;

  @Mock
  private ChangePrimaryKeyColumns changePrimaryKeyColumns;

  @Mock
  private AddTableFrom addTableFrom;

  @Mock
  private AnalyseTable analyseTable;

  @Mock
  private SelectFirstStatement selectFirstStatement;

  @Mock
  private SelectStatement selectStatement;

  @Mock
  private DeleteStatement deleteStatement;

  @Mock
  private InsertStatement insertStatement;

  @Mock
  private InputField inputField;

  @Mock
  private MergeStatement mergeStatement;

  @Mock
  private PortableSqlStatement portableSqlStatement;

  @Mock
  private TruncateStatement truncateStatement;

  @Mock
  private UpdateStatement updateStatement;

  @Mock
  private UnionSetOperator unionSetOperator;

  @Mock
  private MinusSetOperator minusSetOperator;

  @Mock
  private BracketedExpression bracketedExpression;

  @Mock
  private CaseStatement caseStatement;

  @Mock
  private Cast cast;

  @Mock
  private ConcatenatedField concatenatedField;

  @Mock
  private Criterion criterion;

  @Mock
  private FieldFromSelect fieldFromSelect;

  @Mock
  private FieldFromSelectFirst fieldFromSelectFirst;

  @Mock
  private FieldLiteral fieldLiteral;

  @Mock
  private FieldReference fieldReference;

  @Mock
  private Join join;

  @Mock
  private MathsField mathsField;

  @Mock
  private SqlParameter sqlParameter;

  @Mock
  private WhenCondition whenCondition;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  /**
   * Note that not all potential visited classes are tested. Few are final and
   * cannot be mocked.
   */
  @Test
  public void defaultBehaviourIsToDoNothing() {
    // given
    SchemaAndDataChangeVisitor visitor = new SchemaAndDataChangeVisitor() {};

    // when
    visitor.visit(addColumn);
    visitor.visit(addTable);
    visitor.visit(removeTable);
    visitor.visit(addIndex);
    visitor.visit(changeColumn);
    visitor.visit(removeColumn);
    visitor.visit(removeIndex);
    visitor.visit(changeIndex);
    visitor.visit(renameIndex);
    visitor.visit(renameTable);
    visitor.visit(changePrimaryKeyColumns);
    visitor.visit(addTableFrom);
    visitor.visit(analyseTable);
    visitor.visit(selectFirstStatement);
    visitor.visit(selectStatement);
    visitor.visit(deleteStatement);
    visitor.visit(insertStatement);
    visitor.visit(inputField);
    visitor.visit(mergeStatement);
    visitor.visit(portableSqlStatement);
    visitor.visit(truncateStatement);
    visitor.visit(updateStatement);
    visitor.visit(unionSetOperator);
    visitor.visit(minusSetOperator);
    visitor.visit(bracketedExpression);
    visitor.visit(caseStatement);
    visitor.visit(cast);
    visitor.visit(concatenatedField);
    visitor.visit(criterion);
    visitor.visit(fieldFromSelect);
    visitor.visit(fieldFromSelectFirst);
    visitor.visit(fieldLiteral);
    visitor.visit(fieldReference);
    visitor.visit(join);
    visitor.visit(mathsField);
    visitor.visit(sqlParameter);
    visitor.visit(whenCondition);

    // then
    verifyNoInteractions(addColumn, addTable, removeTable, addIndex, changeColumn, removeColumn, removeIndex, changeIndex,
      renameIndex, renameTable, changePrimaryKeyColumns, addTableFrom, analyseTable, selectFirstStatement, selectStatement,
      deleteStatement, insertStatement, inputField, mergeStatement, portableSqlStatement, truncateStatement, updateStatement,
      unionSetOperator, minusSetOperator, bracketedExpression, caseStatement, cast, concatenatedField, criterion, fieldFromSelect,
      fieldFromSelectFirst, fieldLiteral, fieldReference, join, mathsField, sqlParameter, whenCondition);
  }
}

