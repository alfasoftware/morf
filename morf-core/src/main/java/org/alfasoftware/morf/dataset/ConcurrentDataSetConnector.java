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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * Transmits all data from a {@link DataSetProducer} to a {@link DataSetConsumer}. This
 * class acts as a bridge between the producer interface which is fundamentally a data
 * pull operation and the consumer interface which is fundamentally a data push.
 *
 * <p>
 * This version of the connector works using an {@link ExecutorService} to provide
 * work management and concurrency.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */

public class ConcurrentDataSetConnector {

  /**
   * Number of threads in the executor pool.
   */
  private final int threadCount;

  /**
   * The pool of  producers from which all data will be retrieved and pushed to the consumers.
   */
  protected Pool<DataSetProducer> producerPool;
  /**
   * The pool of consumers for all data retrieved from producers.
   */
  protected Pool<DataSetConsumer> consumerPool;


  /**
   * Creates a new instance of this class.
   *
   * @param producerSupplier The supplier for data to transmit.
   * @param consumerSupplier The supplier for the target to which the data should be sent.
   */
  public ConcurrentDataSetConnector(Supplier<DataSetProducer> producerSupplier, Supplier<DataSetConsumer> consumerSupplier) {
    this(producerSupplier, consumerSupplier, calculateDefaultThreadCount());
  }


  /**
   * Creates a new instance of this class.
   *
   * @param producerSupplier The supplier for data to transmit.
   * @param consumerSupplier The supplier for the target to which the data should be sent.
   * @param threadCount Uses provided number of threads instead of default
   */
  public ConcurrentDataSetConnector(Supplier<DataSetProducer> producerSupplier, Supplier<DataSetConsumer> consumerSupplier, int threadCount) {
    super();
    this.consumerPool = new Pool<>(() ->  {
      DataSetConsumer consumer = consumerSupplier.get();
      consumer.open();
      return consumer;
    },
    c -> c.close(DataSetConsumer.CloseState.COMPLETE));

    this.producerPool = new Pool<>(producerSupplier, DataSetProducer::close);
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
        //This is consistent with Cryo calculation
        return Math.max(1,  Math.min(8, processorCount/2));
    }
  }


  /**
   * Transmits all data from the producer to the consumer.
   */
  public void connect() {
    DataSetConsumer.CloseState closeState = DataSetConsumer.CloseState.INCOMPLETE;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    DataSetProducer producerMetadata =  producerPool.borrow();
    DataSetConsumer consumerMetadata = consumerPool.borrow();
    try {
      for (String tableName : producerMetadata.getSchema().tableNames()) {
         try {
          executor.execute(new DataSetConnectorRunnable(producerPool, consumerPool, tableName));
        } catch (Exception e) {
          executor.shutdownNow();
          throw new RuntimeException("Error connecting table [" + tableName + "]", e);
        }
      }
      executor.shutdown();
      executor.awaitTermination(60, TimeUnit.MINUTES);

      // once we've read all the tables without exception, we're complete and ready to release all resources
      closeState = DataSetConsumer.CloseState.FINALLY_COMPLETE;
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    finally {
      executor.shutdownNow();
      consumerPool.release(consumerMetadata);
      producerPool.release(producerMetadata);
      consumerPool.shutdown();
      producerPool.shutdown();
      if(closeState == DataSetConsumer.CloseState.FINALLY_COMPLETE) {
        consumerMetadata.close(closeState);
      }

    }
  }


  /**
   * Runnable for use in {@link ExecutorService} instances. Connects one table.
   */
  private final class DataSetConnectorRunnable implements Runnable {

    /**
     * The source of the data to transmit.
     */
    private final Pool<DataSetProducer> producerPool;

    /**
     * The target to which the data should be sent.
     */
    private final Pool<DataSetConsumer> consumerPool;

    /**
     * Name of the table to transmit.
     */
    private final String tableName;

    /**
     * Create new instance of this class.
     *
     * @param producerPool The pool of producers for data to transmit.
     * @param consumerPool The pool of consumers to which the data should be sent.
     * @param tableName The name of the table to transmit.
     */
    private DataSetConnectorRunnable(Pool<DataSetProducer> producerPool, Pool<DataSetConsumer> consumerPool, String tableName) {
      this.producerPool = producerPool;
      this.consumerPool = consumerPool;
      this.tableName = tableName;
    }

    /**
     * Transfers the table with the name given in the constructor from the producer to
     * the consumer.
     */
    @Override
    public void run() {
      DataSetProducer producer = producerPool.borrow();
      DataSetConsumer consumer = consumerPool.borrow();
      consumer.table(producer.getSchema().getTable(tableName), producer.records(tableName));
      consumerPool.release(consumer);
      producerPool.release(producer);
      //do not close the producer here, do it only at the end of entire processing via pool shutdown
    }
  }

  private static class Pool<T> {

    private final Supplier<T> supplier;

    private final Consumer<T> destroyer;

    private final Deque<T> queue = new ArrayDeque<>();

    Pool(Supplier<T> supplier, Consumer<T> destroyer) {
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
}