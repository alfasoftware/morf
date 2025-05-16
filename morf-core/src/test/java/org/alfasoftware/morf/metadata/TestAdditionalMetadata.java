package org.alfasoftware.morf.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.Test;

public class TestAdditionalMetadata {

  static class SimpleAdditionalMetadata implements AdditionalMetadata {

    @Override
    public boolean isEmptyDatabase() {
      return false;
    }

    @Override
    public boolean tableExists(String name) {
      return false;
    }

    @Override
    public Table getTable(String name) {
      return null;
    }

    @Override
    public Collection<String> tableNames() {
      return List.of();
    }

    @Override
    public Collection<Table> tables() {
      return List.of();
    }

    @Override
    public boolean viewExists(String name) {
      return false;
    }

    @Override
    public View getView(String name) {
      return null;
    }

    @Override
    public Collection<String> viewNames() {
      return List.of();
    }

    @Override
    public Collection<View> views() {
      return List.of();
    }

    @Override
    public boolean sequenceExists(String name) {
      return false;
    }

    @Override
    public Sequence getSequence(String name) {
      return null;
    }

    @Override
    public Collection<String> sequenceNames() {
      return List.of();
    }

    @Override
    public Collection<Sequence> sequences() {
      return List.of();
    }
  }

  //AdditionalMetadata
  @Test
  public void testAdditionalMetadata() {
    SimpleAdditionalMetadata simple = new SimpleAdditionalMetadata();

    assertThrows("must throw NotImplementedException", NotImplementedException.class, simple::primaryKeyIndexNames);
    assertEquals("ignored indexes map", simple.ignoredIndexes(), Map.of());
  }
}
