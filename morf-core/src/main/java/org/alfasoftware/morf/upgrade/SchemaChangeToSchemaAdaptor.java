package org.alfasoftware.morf.upgrade;

import java.util.List;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Schema;

/**
 * Interface for adapting schema changes, i.e. {@link SchemaChange} implementations.
 *
 * This interface can be used to intercept schema changes from the upgrade steps,
 * and modify them as they are applied to a changing schema via the upgrade path
 * in {@link SchemaChangeSequence}.
 *
 * @author Copyright (c) Alfa Financial Software 2026
 */
public interface SchemaChangeToSchemaAdaptor {

  /**
   * Perform adapt operation on a {@link SchemaChange} instance,
   * with the given {@link Schema} in mind.
   *
   * @param change Schema change operation.
   * @param schema Schema to be considered.
   * @return Adapted schema change operation.
   */
  public default List<SchemaChange> adapt(SchemaChange change, Schema schema) {
    return List.of(change);
  }


  /**
   * Simply uses the default implementation, which is already no-op.
   * By no-op, we mean non-changing: the input is passed through as output.
   */
  public static final class NoOp implements SchemaChangeToSchemaAdaptor {
  }


  /**
   * Combines two {@link SchemaChangeToSchemaAdaptor}s to run the second one on the results of the first one.
   */
  public static final class Combining implements SchemaChangeToSchemaAdaptor {

    private final SchemaChangeToSchemaAdaptor first;
    private final SchemaChangeToSchemaAdaptor second;

    public Combining(SchemaChangeToSchemaAdaptor first, SchemaChangeToSchemaAdaptor second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public List<SchemaChange> adapt(SchemaChange change, Schema schema) {
      return first.adapt(change, schema).stream()
          .flatMap(c -> second.adapt(c, schema).stream())
          .collect(Collectors.toList());
    }
  }
}
