package org.alfasoftware.morf.metadata;

import static org.alfasoftware.morf.metadata.DataSetUtils.record;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordBuilder;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordDecorator;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.LocalDate;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Some tests which process large numbers of {@link DataValueLookup} records highly in parallel
 * to determine if there are GC or performance issues.  These are disabled, since they run for
 * a very time and require a large heap (run with
 * -Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=500).
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
@Ignore
public class TestDataSetUtilsVolume {

  private static final int BATCH_SIZE = 2000;
  private static final int POOL_SIZE = 32;
  private static final int QUEUE_DEPTH = 128;

  private static final Log log = LogFactory.getLog(TestDataSetUtilsVolume.class);

  private static final String INTEGER_COLUMN = "A";
  private static final String STRING_COLUMN = "B";
  private static final String BIG_DECIMAL_COLUMN = "C";
  private static final String BOOLEAN_COLUMN = "D";
  private static final String DATE_COLUMN = "E";
  private static final String LOCAL_DATE_COLUMN = "F";
  private static final String LONG_COLUMN = "H";
  private static final String BLOB_COLUMN = "I";
  private static final String UNTYPED_COLUMN = "J";

  private static final BigDecimal BIG_DECIMAL = new BigDecimal("10.000");

  private AtomicLong timerStart;
  private AtomicLong processed;


  /**
   * Creates and destroys lots of records in such a way that some may end up getting past
   * the eden space.
   */
  @Test
  @Ignore
  public void testSimulateHighVolumeWithStrings() throws InterruptedException {
    BlockingQueue<Iterable<Record>> queue = new ArrayBlockingQueue<>(QUEUE_DEPTH);
    ExecutorService pool = Executors.newFixedThreadPool(POOL_SIZE);
    try {
      timerStart = new AtomicLong(System.currentTimeMillis());
      processed = new AtomicLong(0);
      IntStream.range(0, 10).forEach(i -> pool.execute(() -> produce(queue, this::randomiseValues)));
      IntStream.range(0, 8).forEach(i -> pool.execute(() -> consume(queue, this::readValues)));
    } finally {
      pool.shutdown();
      pool.awaitTermination(3, TimeUnit.MINUTES);
    }
  }


  /**
   * Creates and destroys lots of records in such a way that some may end up getting past
   * the eden space.
   */
  @Test
  @Ignore
  public void testSimulateHighVolumeWithBoxedData() throws InterruptedException {
    BlockingQueue<Iterable<Record>> queue = new ArrayBlockingQueue<>(QUEUE_DEPTH);
    ExecutorService pool = Executors.newFixedThreadPool(POOL_SIZE);
    try {
      timerStart = new AtomicLong(System.currentTimeMillis());
      processed = new AtomicLong(0);
      IntStream.range(0, 10).forEach(i -> pool.execute(() -> produce(queue, this::randomiseObjects)));
      IntStream.range(0, 8).forEach(i -> pool.execute(() -> consume(queue, this::readObjects)));
    } finally {
      pool.shutdown();
      pool.awaitTermination(3, TimeUnit.MINUTES);
    }
  }


  /**
   * Creates and destroys lots of records in such a way that some may end up getting past
   * the eden space.
   */
  @Test
  @Ignore
  public void testSimulateHighVolumeWithBoxedDataAndHintedRowSize() throws InterruptedException {
    BlockingQueue<Iterable<Record>> queue = new ArrayBlockingQueue<>(QUEUE_DEPTH);
    ExecutorService pool = Executors.newFixedThreadPool(POOL_SIZE);
    try {
      timerStart = new AtomicLong(System.currentTimeMillis());
      processed = new AtomicLong(0);
      IntStream.range(0, 10).forEach(i -> pool.execute(() -> produceWithHintedColumnCounts(queue, this::randomiseObjects)));
      IntStream.range(0, 8).forEach(i -> pool.execute(() -> consume(queue, this::readObjects)));
    } finally {
      pool.shutdown();
      pool.awaitTermination(3, TimeUnit.MINUTES);
    }
  }


  private void produce(BlockingQueue<Iterable<Record>> queue, Function<RecordBuilder, Record> randomizer) {
    Thread.currentThread().setName("Producer-" + Thread.currentThread().getId());
    outer: while (true) {
      ArrayList<Record> batch = new ArrayList<>();
      for (int j = 0 ; j < BATCH_SIZE ; j++) {
        batch.add(
          RecordDecorator.of(
            RecordDecorator.of(
              randomizer.apply(record())
            ).setString("XXX", "werwer").setString("YYY", "dfsgsf")
          ).setString("ZZZ", "sdfsdfs")
        );
        if (Thread.currentThread().isInterrupted()) {
          break outer;
        }
      }
      try {
        queue.put(batch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break outer;
      }
    }
  }


  private void produceWithHintedColumnCounts(BlockingQueue<Iterable<Record>> queue, Function<RecordBuilder, Record> randomizer) {
    Thread.currentThread().setName("Producer-" + Thread.currentThread().getId());
    outer: while (true) {
      ArrayList<Record> batch = new ArrayList<>();
      for (int j = 0 ; j < BATCH_SIZE ; j++) {
        batch.add(
          RecordDecorator.ofWithInitialCapacity(
            RecordDecorator.ofWithInitialCapacity(
              randomizer.apply(record().withInitialColumnCount(9)),
              2
            )
              .setString("XXX", "werwer")
              .setString("YYY", "dfsgsf"),
            1
          ).setString("ZZZ", "sdfsdfs")
        );
        if (Thread.currentThread().isInterrupted()) {
          break outer;
        }
      }
      try {
        queue.put(batch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break outer;
      }
    }
  }


  private void consume(BlockingQueue<Iterable<Record>> queue, Consumer<Record> consumer) {
    Thread.currentThread().setName("Consumer-" + Thread.currentThread().getId());
    do {

      Iterable<Record> batch = queue.poll();
      if (batch != null) {
        batch.forEach(c -> {
          consumer.accept(c);
          processed.incrementAndGet();
        });
      }

      long currentTime = System.currentTimeMillis();
      long timeFrom = timerStart.get();
      long timeElapsed = currentTime - timerStart.get();

      if (timeElapsed > 5000 && timerStart.compareAndSet(timeFrom, currentTime)) {
        double rate = processed.get();
        rate = rate * 1000 / timeElapsed;
        log.info(String.format("Consume data rate = %.2f/s", rate));
        processed.set(0);

      }
    } while (!Thread.currentThread().isInterrupted());
  }


  @SuppressWarnings("deprecation")
  private void readValues(Record record) {
    record.getValue(INTEGER_COLUMN);
    record.getValue(STRING_COLUMN);
    record.getValue(BIG_DECIMAL_COLUMN);
    record.getValue(DATE_COLUMN);
    record.getValue(LOCAL_DATE_COLUMN);
    record.getValue(LONG_COLUMN);
    record.getValue(BLOB_COLUMN);
    record.getValue(UNTYPED_COLUMN);
  }


  @SuppressWarnings("deprecation")
  private void readObjects(Record record) {
    record.getInteger(INTEGER_COLUMN);
    record.getString(STRING_COLUMN);
    record.getBigDecimal(BIG_DECIMAL_COLUMN);
    record.getDate(DATE_COLUMN);
    record.getLocalDate(LOCAL_DATE_COLUMN);
    record.getLong(LONG_COLUMN);
    record.getByteArray(BLOB_COLUMN);
    record.getValue(UNTYPED_COLUMN);
  }


  @SuppressWarnings("deprecation")
  private RecordBuilder randomiseObjects(RecordBuilder builder) {
    return builder
        .setInteger(INTEGER_COLUMN, RandomUtils.nextInt())
        .setString(STRING_COLUMN, Long.toString(RandomUtils.nextLong()))
        .setBigDecimal(BIG_DECIMAL_COLUMN, BIG_DECIMAL)
        .setBoolean(BOOLEAN_COLUMN, RandomUtils.nextBoolean())
        .setDate(DATE_COLUMN, java.sql.Date.valueOf(java.time.LocalDate.now()))
        .setLocalDate(LOCAL_DATE_COLUMN, LocalDate.now())
        .setLong(LONG_COLUMN, RandomUtils.nextLong())
        .setByteArray(BLOB_COLUMN, Long.toString(RandomUtils.nextLong()).getBytes())
        .value(UNTYPED_COLUMN, Long.toString(RandomUtils.nextLong()));
  }


  @SuppressWarnings("deprecation")
  private RecordBuilder randomiseValues(RecordBuilder builder) {
    return builder
        .value(INTEGER_COLUMN, Integer.toString(RandomUtils.nextInt()))
        .value(STRING_COLUMN, Long.toString(RandomUtils.nextLong()))
        .value(BIG_DECIMAL_COLUMN, BIG_DECIMAL.toPlainString())
        .value(BOOLEAN_COLUMN, Boolean.toString(RandomUtils.nextBoolean()))
        .value(DATE_COLUMN, java.sql.Date.valueOf(java.time.LocalDate.now()).toString())
        .value(LOCAL_DATE_COLUMN, LocalDate.now().toString())
        .value(LONG_COLUMN, Long.toString(RandomUtils.nextLong()))
        .value(BLOB_COLUMN, Long.toString(RandomUtils.nextLong()))
        .value(UNTYPED_COLUMN, Long.toString(RandomUtils.nextLong()));
  }
}