package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.MergeStatement.InputField;
import org.alfasoftware.morf.sql.ExceptSetOperator;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UnionSetOperator;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.*;

/**
 * Visitor of the schema and data changes.
 *
 * @author Copyright (c) Alfa Financial Software 2022
 */
@SuppressWarnings("unused")
public interface SchemaAndDataChangeVisitor {

  /**
   * Perform visit operation on an {@link AddColumn} instance.
   *
   * @param addColumn instance of {@link AddColumn} to visit.
   */
  default void visit(AddColumn addColumn) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on an {@link AddTable} instance.
   *
   * @param addTable instance of {@link AddTable} to visit.
   */
  default void visit(AddTable addTable) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on an {@link RemoveTable} instance.
   *
   * @param removeTable instance of {@link RemoveTable} to visit.
   */
  default void visit(RemoveTable removeTable) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on an {@link AddSequence} instance.
   *
   * @param addSequence instance of {@link AddSequence} to visit.
   */
  default void visit(AddSequence addSequence) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on an {@link RemoveSequence} instance.
   *
   * @param removeSequence instance of {@link RemoveSequence} to visit.
   */
  default void visit(RemoveSequence removeSequence) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on an {@link AddIndex} instance.
   *
   * @param addIndex instance of {@link AddIndex} to visit.
   */
  default void visit(AddIndex addIndex) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on an {@link ChangeColumn} instance.
   *
   * @param changeColumn instance of {@link ChangeColumn} to visit.
   */
  default void visit(ChangeColumn changeColumn) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link RemoveColumn} instance.
   *
   * @param removeColumn instance of {@link RemoveColumn} to visit.
   */
  default void visit(RemoveColumn removeColumn) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link RemoveIndex} instance.
   *
   * @param removeIndex instance of {@link RemoveIndex} to visit.
   */
  default void visit(RemoveIndex removeIndex) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link ChangeIndex} instance.
   *
   * @param changeIndex instance of {@link ChangeIndex} to visit.
   */
  default void visit(ChangeIndex changeIndex) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link RenameIndex} instance.
   *
   * @param renameIndex instance of {@link RenameIndex} to visit.
   */
  default void visit(RenameIndex renameIndex) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link RenameTable} instance.
   *
   * @param renameTable instance of {@link RenameTable} to visit.
   */
  default void visit(RenameTable renameTable) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link ChangePrimaryKeyColumns} instance.
   *
   * @param changePrimaryKeyColumns instance of {@link ChangePrimaryKeyColumns} to visit.
   */
  default void visit(ChangePrimaryKeyColumns changePrimaryKeyColumns) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link AddTableFrom} instance.
   *
   * @param addTableFrom instance of {@link AddTableFrom} to visit.
   */
  default void visit(AddTableFrom addTableFrom) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link AnalyseTable} instance.
   *
   * @param analyseTable instance of {@link AnalyseTable} to visit.
   */
  default void visit(AnalyseTable analyseTable) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link SelectFirstStatement} instance.
   *
   * @param selectFirstStatement instance of {@link SelectFirstStatement} to visit.
   */
  default void visit(SelectFirstStatement selectFirstStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link SelectStatement} instance.
   *
   * @param selectStatement instance of {@link SelectStatement} to visit.
   */
  default void visit(SelectStatement selectStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link DeleteStatement} instance.
   *
   * @param deleteStatement instance of {@link DeleteStatement} to visit.
   */
  default void visit(DeleteStatement deleteStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link InsertStatement} instance.
   *
   * @param insertStatement instance of {@link InsertStatement} to visit.
   */
  default void visit(InsertStatement insertStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link InputField} instance.
   *
   * @param inputField instance of {@link InputField} to visit.
   */
  default void visit(InputField inputField) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link MergeStatement} instance.
   *
   * @param mergeStatement instance of {@link MergeStatement} to visit.
   */
  default void visit(MergeStatement mergeStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link PortableSqlStatement} instance.
   *
   * @param portableSqlStatement instance of {@link PortableSqlStatement} to visit.
   */
  default void visit(PortableSqlStatement portableSqlStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link TruncateStatement} instance.
   *
   * @param truncateStatement instance of {@link TruncateStatement} to visit.
   */
  default void visit(TruncateStatement truncateStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link UpdateStatement} instance.
   *
   * @param updateStatement instance of {@link UpdateStatement} to visit.
   */
  default void visit(UpdateStatement updateStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link UnionSetOperator} instance.
   *
   * @param unionSetOperator instance of {@link UnionSetOperator} to visit.
   */
  default void visit(UnionSetOperator unionSetOperator) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link ExceptSetOperator} instance.
   *
   * @param exceptSetOperator instance of {@link ExceptSetOperator} to visit.
   */
  default void visit(ExceptSetOperator exceptSetOperator) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link BracketedExpression} instance.
   *
   * @param bracketedExpression instance of {@link BracketedExpression} to visit.
   */
  default void visit(BracketedExpression bracketedExpression) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link CaseStatement} instance.
   *
   * @param caseStatement instance of {@link CaseStatement} to visit.
   */
  default void visit(CaseStatement caseStatement) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link Cast} instance.
   *
   * @param cast instance of {@link Cast} to visit.
   */
  default void visit(Cast cast) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link ConcatenatedField} instance.
   *
   * @param concatenatedField instance of {@link ConcatenatedField} to visit.
   */
  default void visit(ConcatenatedField concatenatedField) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link Criterion} instance.
   *
   * @param criterion instance of {@link Criterion} to visit.
   */
  default void visit(Criterion criterion) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link FieldFromSelect} instance.
   *
   * @param fieldFromSelect instance of {@link FieldFromSelect} to visit.
   */
  default void visit(FieldFromSelect fieldFromSelect) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link FieldFromSelectFirst} instance.
   *
   * @param fieldFromSelectFirst instance of {@link FieldFromSelectFirst} to visit.
   */
  default void visit(FieldFromSelectFirst fieldFromSelectFirst) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link FieldLiteral} instance.
   *
   * @param fieldLiteral instance of {@link FieldLiteral} to visit.
   */
  default void visit(FieldLiteral fieldLiteral) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link FieldReference} instance.
   *
   * @param fieldReference instance of {@link FieldReference} to visit.
   */
  default void visit(FieldReference fieldReference) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link Function} instance.
   *
   * @param function instance of {@link Function} to visit.
   */
  default void visit(Function function) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link Join} instance.
   *
   * @param join instance of {@link Join} to visit.
   */
  default void visit(Join join) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link MathsField} instance.
   *
   * @param mathsField instance of {@link MathsField} to visit.
   */
  default void visit(MathsField mathsField) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link SqlParameter} instance.
   *
   * @param sqlParameter instance of {@link SqlParameter} to visit.
   */
  default void visit(SqlParameter sqlParameter) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link SequenceReference} instance.
   *
   * @param sequenceReference instance of {@link SequenceReference} to visit.
   */
  default void visit(SequenceReference sequenceReference) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link WhenCondition} instance.
   *
   * @param whenCondition instance of {@link WhenCondition} to visit.
   */
  default void visit(WhenCondition whenCondition) {
    // Do nothing on default
  }


  /**
   * Perform visit operation on a {@link WindowFunction} instance.
   *
   * @param windowFunction instance of {@link WindowFunction} to visit.
   */
  default void visit(WindowFunction windowFunction) {
    // Do nothing on default
  }
}
