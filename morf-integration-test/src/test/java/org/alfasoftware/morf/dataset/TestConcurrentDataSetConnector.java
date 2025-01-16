package org.alfasoftware.morf.dataset;

import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.function.Supplier;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Test;

/**
 * Test that the concurrent data set connector works as expected.
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public class TestConcurrentDataSetConnector {


  @Test
  public void testBasicDataSetTransmissionSingleThread() {
    ConsumerTestSupplier consumerTestSupplier = new ConsumerTestSupplier();

    MockDataSetProducer testProducer = new MockDataSetProducer();
    testProducer.addTable(table("foo").columns(
        idColumn(),
        versionColumn(),
        column("bar", DataType.STRING, 10),
        column("baz", DataType.STRING, 10)
      )
    );

    Table metaData = table("foo2").columns(
      idColumn(),
      versionColumn(),
      column("bar", DataType.STRING, 10),
      column("baz", DataType.STRING, 10)
    );
    testProducer.addTable(metaData, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar", "val1")
      .setString("baz", "val2"));

    new ConcurrentDataSetConnector(testProducer, consumerTestSupplier, 1).connect();

    assertEquals("Data set connector", "[open, close, close]", consumerTestSupplier.testConsumer1.toString());
    assertEquals("Data set connector", "[open, table foo [column id, column version, column bar, column baz], end table, table foo2 [column id, column version, column bar, column baz], [1, 1, val1, val2], end table, close]", consumerTestSupplier.testConsumer2.toString());
  }


  @Test
  public void testBasicDataSetTransmissionMultiThreaded() {
    ConsumerTestSupplier consumerTestSupplier = new ConsumerTestSupplier();

    MockDataSetProducer testProducer = new MockDataSetProducer();
    testProducer.addTable(table("foo").columns(
        idColumn(),
        versionColumn(),
        column("bar", DataType.STRING, 10),
        column("baz", DataType.STRING, 10)
      )
    );

    Table metaData = table("foo2").columns(
      idColumn(),
      versionColumn(),
      column("bar2", DataType.STRING, 10),
      column("baz2", DataType.STRING, 10)
    );
    testProducer.addTable(metaData, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar2", "val1")
      .setString("baz2", "val2"));

    new ConcurrentDataSetConnector(testProducer, consumerTestSupplier, 3).connect();

    //we don't care about ordering, only checking that all is processed
    String allResults =  consumerTestSupplier.testConsumer1.toString() + consumerTestSupplier.testConsumer2.toString() + consumerTestSupplier.testConsumer3.toString();
    verifyConcurrent(allResults);
  }


  @Test
  public void testAutomaticThreadNumberAllocation() {
    ConsumerTestSupplier consumerTestSupplier = new ConsumerTestSupplier();

    MockDataSetProducer testProducer = new MockDataSetProducer();
    testProducer.addTable(table("foo").columns(
        idColumn(),
        versionColumn(),
        column("bar", DataType.STRING, 10),
        column("baz", DataType.STRING, 10)
      )
    );

    Table metaData = table("foo2").columns(
      idColumn(),
      versionColumn(),
      column("bar2", DataType.STRING, 10),
      column("baz2", DataType.STRING, 10)
    );
    testProducer.addTable(metaData, record()
      .setInteger(idColumn().getName(), 1)
      .setInteger(versionColumn().getName(), 1)
      .setString("bar2", "val1")
      .setString("baz2", "val2"));

    //do not pass thread number
    new ConcurrentDataSetConnector(testProducer, consumerTestSupplier).connect();

    //we don't care about ordering, only checking that all is processed
    String allResults =  consumerTestSupplier.testConsumer1.toString() + consumerTestSupplier.testConsumer2.toString() + consumerTestSupplier.testConsumer3.toString();
    verifyConcurrent(allResults);
  }

  private void verifyConcurrent(String allResults) {
    assertTrue(allResults.contains("open"));
    assertTrue(allResults.contains("foo"));
    assertTrue(allResults.contains("foo2"));
    assertTrue(allResults.contains("bar"));
    assertTrue(allResults.contains("bar2"));
    assertTrue(allResults.contains("baz"));
    assertTrue(allResults.contains("baz2"));
    assertTrue(allResults.contains("val1"));
    assertTrue(allResults.contains("val2"));
    assertTrue(allResults.contains("close"));
  }


  private static class ConsumerTestSupplier implements Supplier<DataSetConsumer> {

    MockDataSetConsumer testConsumer1 = new MockDataSetConsumer();
    MockDataSetConsumer testConsumer2 = new MockDataSetConsumer();
    MockDataSetConsumer testConsumer3 = new MockDataSetConsumer();
    int internalCount = 0;

    @Override
    public DataSetConsumer get() {
      internalCount++;
      switch (internalCount){
        case 1: return testConsumer1;
        case 2: return testConsumer2;
        case 3: return testConsumer3;
        default: throw new IllegalStateException();
      }
    }
  }
}