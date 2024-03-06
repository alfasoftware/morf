package org.alfasoftware.morf.upgrade;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Sequence;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSequenceChanges {

  private static final Function<Sequence, String> TO_NAME = new Function<Sequence, String> () {
    @Override public String apply(Sequence sequence) {
      return sequence.getName();
    }
  };


  /**
   * Test a simple configuration.
   */
  @Test
  public void testSimple() {
    SequenceChanges c = new SequenceChanges(
      ImmutableSet.of(
        sequence("A"),
        sequence("B"),
        sequence("C"),
        sequence("D")
      ),
      ImmutableSet.of(
        sequence("B")
      ),
      ImmutableSet.of(
        sequence("B")
      ));

    assertEquals("Views to drop mismatch", ImmutableSet.of("B"), nameSet(c.getSequencesToDrop()));
    assertEquals("Views to deploy mismatch", ImmutableSet.of("B"), nameSet(c.getSequencesToDeploy()));

  }


  /**
   * Test we're still prepared to drop sequences that we know nothing of.
   */
  @Test
  public void testObsoleted() {
    SequenceChanges c = new SequenceChanges(
      ImmutableSet.of(
        sequence("A"),
        sequence("B"),
        sequence("C")
      ),
      ImmutableSet.of(
        sequence("B"),
        sequence("X")
      ),
      ImmutableSet.of(
        sequence("B")
      ));

    assertEquals("Views to drop mismatch", ImmutableSet.of("B", "X"), nameSet(c.getSequencesToDrop()));
    assertEquals("Views to deploy mismatch", ImmutableSet.of("B"), nameSet(c.getSequencesToDeploy()));
  }


  /**
   * Ensure that dropping of discovered views is case-insensitive.
   */
  @Test
  public void testCaseCorrectionOfDropSet() {
    SequenceChanges c = new SequenceChanges(
      ImmutableSet.of(
        sequence("A"),
        sequence("B")
      ),
      ImmutableSet.of(
        sequence("b")
      ),
      ImmutableSet.of(
        sequence("A"),
        sequence("B")
      ));

    assertEquals("Sequences to drop mismatch", ImmutableSet.of("B"), nameSet(c.getSequencesToDrop()));
    assertEquals("Sequences to deploy mismatch", ImmutableSet.of("A", "B"), nameSet(c.getSequencesToDeploy()));

    Ordering<String> deployOrder = Ordering.explicit(nameList(c.getSequencesToDeploy()));
    assertTrue("Must deploy A before B", deployOrder.compare("A", "B") < 0);
  }


  /**
   * Create a sequence.
   *
   * @param name sequence name.
   * @return named sequence with dependencies.
   */
  private Sequence sequence(String name) {
    return SchemaUtils.sequence(name, 1, false);
  }


  /**
   * Convenience function - sequence collection to set of names.
   * @param sequences collection of sequences.
   * @return set of names of sequences.
   */
  private Set<String> nameSet(Collection<Sequence> sequences) {
    return ImmutableSet.copyOf(Collections2.transform(sequences, TO_NAME));
  }


  /**
   * Convenience function - sequence collection to list of names (preserving order).
   * @param sequences collection of sequences (assumed to be ordered)
   * @return list of names of sequences.
   */
  private List<String> nameList(Collection<Sequence> sequences) {
    return ImmutableList.copyOf(Collections2.transform(sequences, TO_NAME));
  }

}
