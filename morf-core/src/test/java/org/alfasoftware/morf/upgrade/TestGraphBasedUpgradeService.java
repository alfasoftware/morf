package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.alfasoftware.morf.upgrade.GraphBasedUpgradeService.GraphBasedUpgradeServiceFactory;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * All tests of {@link GraphBasedUpgradeService}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestGraphBasedUpgradeService {
  private GraphBasedUpgradeNode node1, node2, root;
  private GraphBasedUpgradeService service;

  @Before
  public void setup() {
    // given
    node1 = new GraphBasedUpgradeNode("1", 1L, Sets.newHashSet(), Sets.newHashSet(), false);
    node2 = new GraphBasedUpgradeNode("2", 2L, Sets.newHashSet(), Sets.newHashSet(), false);
    root = new GraphBasedUpgradeNode("root", 0L, Sets.newHashSet(), Sets.newHashSet(), false);
    root.getChildren().add(node1);
    node1.getParents().add(root);
    node1.getChildren().add(node2);
    node2.getParents().add(node1);

    GraphBasedUpgrade upgrade = new GraphBasedUpgrade(root, Lists.newArrayList(), Lists.newArrayList(), 2);
    service = new GraphBasedUpgradeService(upgrade);
  }


  @Test
  public void testNewNodeAvailable() {
    // then
    assertFalse(service.allNodesCompleted());
    assertEquals(node1, service.nextNode().get());
  }


  @Test
  public void testCompleteNode() {
    // when
    GraphBasedUpgradeNode nextNode = service.nextNode().get();

    // then
    assertEquals(node1, nextNode);
    assertFalse(service.allNodesCompleted());

    // when
    service.completeNode(node1);

    // then
    assertFalse(service.allNodesCompleted());

    // when
    nextNode = service.nextNode().get();

    // then
    assertEquals(node2, nextNode);
    assertFalse(service.allNodesCompleted());
  }


  @Test
  public void testNoNewNodesAvailable() {
    // when
    GraphBasedUpgradeNode nextNode = service.nextNode().get();

    // then
    assertEquals(node1, nextNode);
    assertFalse(service.allNodesCompleted());

    // when
    Optional<GraphBasedUpgradeNode> empty = service.nextNode();

    // then
    assertFalse(empty.isPresent());
    assertFalse(service.allNodesCompleted());
  }


  @Test(timeout = 1000)
  public void testWaitingForAllNodesToBeCompleted() throws InterruptedException {
    // given
    final AtomicBoolean stillWaiting= new AtomicBoolean(true);
    Thread waiting = new Thread(new Runnable() {
      @Override
      public void run() {
        service.waitForAllNodesToBeCompleted();
        stillWaiting.set(false);
      }
    });

    // when then
    waiting.start();
    Thread.sleep(100);
    assertTrue(stillWaiting.get());
    GraphBasedUpgradeNode nextNode = service.nextNode().get();
    service.completeNode(nextNode);
    nextNode = service.nextNode().get();
    service.completeNode(nextNode);
    waiting.join();
    assertTrue(service.allNodesCompleted());
    assertFalse(stillWaiting.get());
  }


  @Test(timeout = 1000)
  public void testWaitingForNewNodeAvailable() throws InterruptedException {
    // given
    final AtomicBoolean stillWaiting= new AtomicBoolean(true);
    Thread waiting = new Thread(new Runnable() {
      @Override
      public void run() {
        service.waitForReadyToExecuteNode();
        stillWaiting.set(false);
      }
    });

    // when then
    final GraphBasedUpgradeNode nextNode = service.nextNode().get();
    waiting.start();
    assertTrue(stillWaiting.get());
    Thread.sleep(100);
    service.completeNode(nextNode);
    waiting.join();
    assertFalse(stillWaiting.get());
  }


  @Test
  public void testFactory() {
    GraphBasedUpgradeServiceFactory factory = new GraphBasedUpgradeServiceFactory();
    assertNotNull(factory.create(new GraphBasedUpgrade(root, Lists.newArrayList(), Lists.newArrayList(), 2)));
  }
}
