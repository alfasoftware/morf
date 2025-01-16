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
package org.alfasoftware.morf.dataset;

import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Transmits all data from a {@link DataSetProducer} to a {@link DataSetConsumer}. This
 * class acts as a bridge between the producer interface which is fundamentally a data
 * pull operation and the consumer interface which is fundamentally a data push.
 *
 * <p>
 * This version of the connector works using an {@link ExecutorService} to provide
 * work management and concurrency. It is expecting a thread safe implementation of {@link DataSetProducer}
 * and a supplier of consumer instances to be executed in parallel.
 * </p>
 *
 */

public class ConcurrentDataSetConnector {

  private static final Log log = LogFactory.getLog(ConcurrentDataSetConnector.class);
  /**
   * Number of threads in the executor pool.
   */
  private final int threadCount;

  /**
   * Producer from which all data will be retrieved and pushed to the consumers.
   */
  private final DataSetProducer producer;

  /**
   * The pool of consumers for all data retrieved from the producer.
   */
  private final Pool<DataSetConsumer> consumerPool;

  private final AtomicInteger processedTableCount = new AtomicInteger();


  /**
   * Creates a new instance of this class.
   *
   * @param producer The producer of data to transmit.
   * @param consumerSupplier The supplier for the target to which the data should be sent.
   */
  public ConcurrentDataSetConnector(DataSetProducer producer, Supplier<DataSetConsumer> consumerSupplier) {
    this(producer, consumerSupplier, calculateDefaultThreadCount());
  }


  /**
   * Creates a new instance of this class.
   *
   * @param producer Producer of data to transmit.
   * @param consumerSupplier The supplier for the target to which the data should be sent.
   * @param threadCount Uses provided number of threads instead of default
   */
  public ConcurrentDataSetConnector(DataSetProducer producer, Supplier<DataSetConsumer> consumerSupplier, int threadCount) {
    super();
    this.consumerPool = new Pool<>(() ->  {
      DataSetConsumer consumer = consumerSupplier.get();
      consumer.open();
      return consumer;
    },
    c -> c.close(DataSetConsumer.CloseState.COMPLETE));

    this.producer = producer;
    this.threadCount = threadCount;
  }


  /**
   * Calculates the number of threads to use
   */
  private static int calculateDefaultThreadCount() {
    int processorCount = Runtime.getRuntime().availableProcessors();

    switch(processorCount) {
      case 0:
        throw new IllegalStateException("Could not find at least 1 processor");
      case 1:
        return 1;
      default:
        return Math.max(1,  Math.min(8, processorCount/2));
    }
  }


  /**
   * Transmits all data from the producer to the consumer.
   */
  public void connect() {
    DataSetConsumer.CloseState closeState = DataSetConsumer.CloseState.INCOMPLETE;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    DataSetConsumer consumerMetadata = consumerPool.borrow();

    long startTime = System.currentTimeMillis();
    Logger logger = null;
    try {
      producer.open();
      Collection<String> tableNames = producer.getSchema().tableNames();
      logger = new Logger(processedTableCount, tableNames.size());
      new Thread(logger).start();

      tableNames.forEach((String tableName) -> executeNewRunnable(executor, producer, consumerPool, tableName));
      executor.shutdown();
      executor.awaitTermination(60, TimeUnit.MINUTES);
      logger.shouldContinue = false;

      // once we've read all the tables without exception, we're complete and ready to release all resources
      closeState = DataSetConsumer.CloseState.FINALLY_COMPLETE;
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    finally {
      executor.shutdownNow();
      consumerPool.release(consumerMetadata);
      consumerPool.shutdown();
      producer.close();
      if(closeState == DataSetConsumer.CloseState.FINALLY_COMPLETE) {
        consumerMetadata.close(closeState);
      }
      if(logger != null) {
        logger.shouldContinue = false;
      }
    }
    long finishTime = System.currentTimeMillis();
    float totalSec = (finishTime - startTime) / 1000F;
    log.info("Finished data set import. ThreadCount="+ threadCount + ", totalTimeSec=" + totalSec);
  }


  private void executeNewRunnable(ExecutorService executor, DataSetProducer producer, Pool<DataSetConsumer> consumerPool, String tableName) {
    try {
      executor.execute(new DataSetConnectorRunnable(producer, consumerPool, tableName, processedTableCount));
    } catch (Exception e) {
      executor.shutdownNow();
      throw new RuntimeException("Error connecting table [" + tableName + "]", e);
    }
  }


  /**
   * Runnable for use in {@link ExecutorService} instances. Connects one table.
   */
  private final class DataSetConnectorRunnable implements Runnable {

    /**
     * The source of the data to transmit.
     */
    private final DataSetProducer producer;

    /**
     * The target to which the data should be sent.
     */
    private final Pool<DataSetConsumer> consumerPool;

    /**
     * Name of the table to transmit.
     */
    private final String tableName;

    private final AtomicInteger processedTableCount;

    /**
     * Create new instance of this class.
     *
     * @param producer The producer for data to transmit.
     * @param consumerPool The pool of consumers to which the data should be sent.
     * @param tableName The name of the table to transmit.
     */
    private DataSetConnectorRunnable(DataSetProducer producer, Pool<DataSetConsumer> consumerPool,
                                     String tableName, AtomicInteger processedTableCount) {
      this.producer = producer;
      this.consumerPool = consumerPool;
      this.tableName = tableName;
      this.processedTableCount = processedTableCount;
    }


    /**
     * Transfers the table with the name given in the constructor from the producer to
     * the consumer.
     */
    @Override
    public void run() {
      DataSetConsumer consumer = consumerPool.borrow();
      consumer.table(producer.getSchema().getTable(tableName), producer.records(tableName));
      processedTableCount.incrementAndGet();
      consumerPool.release(consumer);
    }
  }


  private static final class Pool<T> {

    private final Supplier<T> supplier;

    private final Consumer<T> destroyer;

    private final Deque<T> queue = new ConcurrentLinkedDeque<>();

    private Pool(Supplier<T> supplier, Consumer<T> destroyer) {
      this.supplier = supplier;
      this.destroyer = destroyer;
    }

    public T borrow() {
      T next = queue.poll();
      if (next == null) {
        next = supplier.get();
      }
      return next;
    }

    public void release(T item) {
      queue.add(item);
    }

    public void shutdown() {
      queue.forEach(destroyer);
    }
  }


  private static final class Logger implements Runnable {

    private static final Log log = LogFactory.getLog(Logger.class);

    private final AtomicInteger processedTableCount;

    private final int totalTableCount;

    private volatile boolean shouldContinue = true;

    private Logger(AtomicInteger processedTableCount, int totalTableCount) {
      this.processedTableCount = processedTableCount;
      this.totalTableCount = totalTableCount;
    }


    @Override
    public void run() {
      while(shouldContinue) {
        logInternal();
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          log.warn("Interrupted the logger thread");
          shouldContinue = false;
          Thread.currentThread().interrupt();
        }
      }
      logInternal(); //flush the last log statement
    }

    private void logInternal() {
      log.info("Processed [" + processedTableCount.get() + "/" + totalTableCount + "] tables");
    }
  }
}