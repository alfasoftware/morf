package org.alfasoftware.morf.upgrade;

/**
 * Interface for adapting schema changes, i.e. {@link SchemaChange} implementations.
 * This interface can be used to intercept schema changes from the upgrade steps,
 * and modified before they are used for upgrade path in {@link SchemaChangeSequence}.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public interface SchemaChangeAdaptor {

  /**
   * Perform adapt operation on an {@link AddColumn} instance.
   *
   * @param addColumn instance of {@link AddColumn} to adapt.
   */
  public default AddColumn adapt(AddColumn addColumn) {
    return addColumn;
  }


  /**
   * Perform adapt operation on an {@link AddTable} instance.
   *
   * @param addTable instance of {@link AddTable} to adapt.
   */
  public default AddTable adapt(AddTable addTable) {
    return addTable;
  }


  /**
   * Perform adapt operation on an {@link RemoveTable} instance.
   *
   * @param removeTable instance of {@link RemoveTable} to adapt.
   */
  public default RemoveTable adapt(RemoveTable removeTable) {
    return removeTable;
  }


  /**
   * Perform adapt operation on an {@link AddIndex} instance.
   *
   * @param addIndex instance of {@link AddIndex} to adapt.
   */
  public default AddIndex adapt(AddIndex addIndex) {
    return addIndex;
  }


  /**
   * Perform adapt operation on an {@link ChangeColumn} instance.
   *
   * @param changeColumn instance of {@link ChangeColumn} to adapt.
   */
  public default ChangeColumn adapt(ChangeColumn changeColumn) {
    return changeColumn;
  }


  /**
   * Perform adapt operation on a {@link RemoveColumn} instance.
   *
   * @param removeColumn instance of {@link RemoveColumn} to adapt.
   */
  public default RemoveColumn adapt(RemoveColumn removeColumn) {
    return removeColumn;
  }


  /**
   * Perform adapt operation on a {@link RemoveIndex} instance.
   *
   * @param removeIndex instance of {@link RemoveIndex} to adapt.
   */
  public default RemoveIndex adapt(RemoveIndex removeIndex) {
    return removeIndex;
  }


  /**
   * Perform adapt operation on a {@link ChangeIndex} instance.
   *
   * @param changeIndex instance of {@link ChangeIndex} to adapt.
   */
  public default ChangeIndex adapt(ChangeIndex changeIndex) {
    return changeIndex;
  }


  /**
   * Perform adapt operation on a {@link RenameIndex} instance.
   *
   * @param renameIndex instance of {@link RenameIndex} to adapt.
   */
  public default RenameIndex adapt(RenameIndex renameIndex) {
    return renameIndex;
  }


  /**
   * Perform adapt operation on a {@link ExecuteStatement} instance.
   *
   * @param executeStatement instance of {@link ExecuteStatement} to adapt.
   */
  public default ExecuteStatement adapt(ExecuteStatement executeStatement) {
    return executeStatement;
  }


  /**
   * Perform adapt operation on a {@link RenameTable} instance.
   *
   * @param renameTable instance of {@link RenameTable} to adapt.
   */
  public default RenameTable adapt(RenameTable renameTable) {
    return renameTable;
  }

  /**
   * Perform adapt operation on a {@link ChangePrimaryKeyColumns} instance.
   *
   * @param renameTable instance of {@link ChangePrimaryKeyColumns} to adapt.
   */
  public default ChangePrimaryKeyColumns adapt(ChangePrimaryKeyColumns renameTable) {
    return renameTable;
  }


  /**
   * Perform adapt operation on a {@link AddTableFrom} instance.
   *
   * @param addTableFrom instance of {@link AddTableFrom} to adapt.
   */
  public default AddTableFrom adapt(AddTableFrom addTableFrom) {
    return addTableFrom;
  }


  /**
   * Perform adapt operation on a {@link AnalyseTable} instance.
   *
   * @param analyseTable instance of {@link AnalyseTable} to adapt.
   */
  public default AnalyseTable adapt(AnalyseTable analyseTable) {
    return analyseTable;
  }


  /**
   * Perform adapt operation on an {@link AddSequence} instance.
   *
   * @param addSequence instance of {@link AddSequence} to adapt.
   */
  public default AddSequence adapt(AddSequence addSequence) {
    return addSequence;
  }


  /**
   * Perform adapt operation on an {@link RemoveSequence} instance.
   *
   * @param removeSequence instance of {@link RemoveSequence} to adapt.
   */
  public default RemoveSequence adapt(RemoveSequence removeSequence) {
    return removeSequence;
  }


  /**
   * Simply uses the default implementation, which is already no-op.
   * By no-op, we mean non-changing: the input is passed through as output.
   */
  public static final class NoOp implements SchemaChangeAdaptor {
  }


  /**
   * Combines two {@link SchemaChangeAdaptor}s to run the second one on the results of the first one.
   */
  public static final class Combining implements SchemaChangeAdaptor {

    private final SchemaChangeAdaptor first;
    private final SchemaChangeAdaptor second;

    public Combining(SchemaChangeAdaptor first, SchemaChangeAdaptor second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public AddColumn adapt(AddColumn addColumn) {
      return second.adapt(first.adapt(addColumn));
    }

    @Override
    public AddTable adapt(AddTable addTable) {
      return second.adapt(first.adapt(addTable));
    }

    @Override
    public RemoveTable adapt(RemoveTable removeTable) {
      return second.adapt(first.adapt(removeTable));
    }

    @Override
    public AddIndex adapt(AddIndex addIndex) {
      return second.adapt(first.adapt(addIndex));
    }

    @Override
    public ChangeColumn adapt(ChangeColumn changeColumn) {
      return second.adapt(first.adapt(changeColumn));
    }

    @Override
    public RemoveColumn adapt(RemoveColumn removeColumn) {
      return second.adapt(first.adapt(removeColumn));
    }

    @Override
    public RemoveIndex adapt(RemoveIndex removeIndex) {
      return second.adapt(first.adapt(removeIndex));
    }

    @Override
    public ChangeIndex adapt(ChangeIndex changeIndex) {
      return second.adapt(first.adapt(changeIndex));
    }

    @Override
    public RenameIndex adapt(RenameIndex renameIndex) {
      return second.adapt(first.adapt(renameIndex));
    }

    @Override
    public ExecuteStatement adapt(ExecuteStatement executeStatement) {
      return second.adapt(first.adapt(executeStatement));
    }

    @Override
    public RenameTable adapt(RenameTable renameTable) {
      return second.adapt(first.adapt(renameTable));
    }

    @Override
    public ChangePrimaryKeyColumns adapt(ChangePrimaryKeyColumns renameTable) {
      return second.adapt(first.adapt(renameTable));
    }

    @Override
    public AddTableFrom adapt(AddTableFrom addTableFrom) {
      return second.adapt(first.adapt(addTableFrom));
    }

    @Override
    public AnalyseTable adapt(AnalyseTable analyseTable) {
      return second.adapt(first.adapt(analyseTable));
    }

    @Override
    public AddSequence adapt(AddSequence addSequence) {
      return second.adapt(first.adapt(addSequence));
    }

    @Override
    public RemoveSequence adapt(RemoveSequence removeSequence) {
      return second.adapt(first.adapt(removeSequence));
    }
  }
}
