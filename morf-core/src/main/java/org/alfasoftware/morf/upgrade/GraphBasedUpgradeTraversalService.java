package org.alfasoftware.morf.upgrade;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.Inject;

/**
 * <p>
 * Service which encapsulates operations on the {@link GraphBasedUpgrade} which
 * includes progressing through the graph and completing
 * {@link GraphBasedUpgradeNode} executions.
 * </p>
 * <p>
 * The {@link GraphBasedUpgradeTraversalService} is created using the factory and is
 * created for a specific instance of {@link GraphBasedUpgrade}. <b>Since it is
 * tracking specific upgrade the service is stateful.</b>
 * </p>
 * <p>
 * The service is thread safe and can be used by multiple execution threads.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
@ThreadSafe
public class GraphBasedUpgradeTraversalService {
  private static final Log LOG = LogFactory.getLog(GraphBasedUpgradeTraversalService.class);
  private final GraphBasedUpgrade graphBasedUpgrade;
  private final Set<GraphBasedUpgradeNode> readyToExecuteNodes = new HashSet<>();
  private final Set<GraphBasedUpgradeNode> completedNodes = new HashSet<>();
  private final Lock lock = new ReentrantLock();
  private final Condition allNodesCompletedCondition = lock.newCondition();
  private final Condition newReadyToExecuteNode = lock.newCondition();

  /**
   * Constructs {@link GraphBasedUpgradeTraversalService} instance for a specific upgrade
   *
   * @param graphBasedUpgrade for which the service should be created
   */
  GraphBasedUpgradeTraversalService(GraphBasedUpgrade graphBasedUpgrade) {
    this.graphBasedUpgrade = graphBasedUpgrade;
    readyToExecuteNodes.addAll(graphBasedUpgrade.getRoot().getChildren());
  }


  /**
   * @return the next node to be executed if such node is available
   */
  public Optional<GraphBasedUpgradeNode> nextNode() {
    lock.lock();
    try {
      Optional<GraphBasedUpgradeNode> nextNode = readyToExecuteNodes.stream()
          .min(Comparator.comparing(GraphBasedUpgradeNode::getSequence));
      if (nextNode.isPresent()) {
        readyToExecuteNodes.remove(nextNode.get());
        LOG.debug("Returning next node to be processed: " + nextNode.get().getName());
      } else {
        LOG.debug("No node ready to be processed is available.");
      }
      return nextNode;
    } finally {
      lock.unlock();
    }
  }


  /**
   * Informs the service that given {@link GraphBasedUpgradeNode} has been
   * executed.
   *
   * @param node which execution has been completed
   */
  public void completeNode(GraphBasedUpgradeNode node) {
    lock.lock();
    try {
      completedNodes.add(node);
      if(completedNodes.size() == graphBasedUpgrade.getNumberOfNodes()) {
        allNodesCompletedCondition.signalAll();
        LOG.debug("All nodes have been processed.");
      } else {
        // find all children with completed parents and add them to ready list
        Set<GraphBasedUpgradeNode> toExecute = node.getChildren().stream()
            .filter(child -> child.getParents().stream().allMatch(par -> completedNodes.contains(par))).collect(Collectors.toSet());
        if(!toExecute.isEmpty()) {
          readyToExecuteNodes.addAll(toExecute);
          newReadyToExecuteNode.signalAll();
        }
        LOG.debug("Completed node: " + node.getName() + ".\n" + "New nodes enabled for execution (if any): "
            + String.join(",", toExecute.stream().map(GraphBasedUpgradeNode::getName).collect(Collectors.toList())));
      }
    } finally {
      lock.unlock();
    }
  }


  /**
   * @return true if all nodes of the upgrade have been executed
   */
  public boolean allNodesCompleted() {
    lock.lock();
    try {
      return allNodesCompletedNoLock();
    } finally {
      lock.unlock();
    }
  }


  /**
   * @return true if all nodes of the upgrade have been executed
   */
  private boolean allNodesCompletedNoLock() {
    return graphBasedUpgrade.getNumberOfNodes() == completedNodes.size();
  }


  /**
   * Caller of this method will be blocked awaiting moment when at least one new
   * node is available for execution or all the nodes of the upgrade have been
   * executed. When the upgrade reaches that point the block will be released and
   * the method will be completed. Note that the the fact that at the time of the
   * block at least one new node has been available doesn't guarantee that it will
   * still be available later. It may be potentially acquired by another thread.
   */
  public void waitForReadyToExecuteNode() {
    lock.lock();
    try {
      while(readyToExecuteNodes.isEmpty() && !allNodesCompletedNoLock()) {
        newReadyToExecuteNode.await(500, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      LOG.error("InterruptedException in GraphBasedUpgradeService.waitForAllNodesToBeCompleted", e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Factory of {@link GraphBasedUpgradeTraversalService} instances.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  public static class GraphBasedUpgradeTraversalServiceFactory {

    /**
     * Default constructor
     */
    @Inject
    public GraphBasedUpgradeTraversalServiceFactory() {}


    /**
     * Creates new {@link GraphBasedUpgradeTraversalService} for a given
     * {@link GraphBasedUpgrade}.
     *
     * @param graphBasedUpgrade for which the service should be created
     * @return new {@link GraphBasedUpgradeTraversalService} instance
     */
    public GraphBasedUpgradeTraversalService create(GraphBasedUpgrade graphBasedUpgrade) {
      return new GraphBasedUpgradeTraversalService(graphBasedUpgrade);
    }
  }
}
