package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeBuilder.GraphBasedUpgradeBuilderFactory;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeSchemaChangeVisitor.GraphBasedUpgradeSchemaChangeVisitorFactory;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeScriptGenerator.GraphBasedUpgradeScriptGeneratorFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * All tests of {@link GraphBasedUpgradeBuilder}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestGraphBasedUpgradeBuilder {

  @Mock
  private GraphBasedUpgradeSchemaChangeVisitorFactory visitorFactory;

  @Mock
  private GraphBasedUpgradeScriptGeneratorFactory scriptGeneratorFactory;

  @Mock
  private DrawIOGraphPrinter drawIOGraphPrinter;

  @Mock
  private Schema sourceSchema;

  @Mock
  private Schema targetSchema;

  @Mock
  private SqlDialect sqlDialect;

  @Mock
  private ConnectionResources connectionResources;

  @Mock
  private SchemaChangeSequence schemaChangeSequence;

  @Mock
  private ViewChanges viewChanges;

  @Mock
  private UpgradeTableResolution upgradeTableResolution;

  @Mock
  private GraphBasedUpgradeScriptGenerator scriptGen;

  private final Set<String> exclusiveExecutionSteps = new HashSet<>();

  private GraphBasedUpgradeBuilder builder;

  private final List<UpgradeStep> upgradeSteps = new ArrayList<>();

  private UpgradeStep u1, u2, u3, u4, u5, fu1, fu2, fu3, fu4, eu1, eu2, u1000, u1001;

  private final List<String> initialisationSql = new ArrayList<>();

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(connectionResources.sqlDialect()).thenReturn(sqlDialect);

    when(schemaChangeSequence.getUpgradeTableResolution()).thenReturn(upgradeTableResolution);
    when(schemaChangeSequence.getUpgradeSteps()).thenReturn(upgradeSteps);

    when(scriptGeneratorFactory.create(eq(sourceSchema), eq(targetSchema), eq(connectionResources), any(Table.class), eq(viewChanges), eq(initialisationSql))).thenReturn(scriptGen);
    when(scriptGen.generatePostUpgradeStatements()).thenReturn(Lists.newArrayList("post"));
    when(scriptGen.generatePreUpgradeStatements()).thenReturn(Lists.newArrayList("pre"));

    u1 = new U1();
    u2 = new U2();
    u3 = new U3();
    u4 = new U4();
    u5 = new U5();
    fu1 = new FU1();
    fu2 = new FU2();
    fu3 = new FU3();
    fu4 = new FU4();
    eu1 = new EU1();
    eu2 = new EU2();
    u1000 = new U1000();
    u1001 = new U1001();

    builder = new GraphBasedUpgradeBuilder(visitorFactory, scriptGeneratorFactory, drawIOGraphPrinter, sourceSchema, targetSchema,
        connectionResources, exclusiveExecutionSteps, schemaChangeSequence, viewChanges);
  }


  /**
   * Standard write relationships
   */
  @Test
  public void testBasicAutomaticallyAnalyzedModifies() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1.class.getName())).thenReturn(Sets.newHashSet("t1", "t2"));
    when(upgradeTableResolution.getModifiedTables(U2.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getModifiedTables(U3.class.getName())).thenReturn(Sets.newHashSet("t2"));
    when(upgradeTableResolution.getModifiedTables(U4.class.getName())).thenReturn(Sets.newHashSet("t1", "t2"));

    upgradeSteps.addAll(Lists.newArrayList(u1, u2, u3, u4));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, u1, u2);
    checkParentChild(upgrade, u1, u3);
    checkParentChild(upgrade, u2, u4);
    checkParentChild(upgrade, u3, u4);

    checkNotParentChild(upgrade, u1, u4);
    checkNotParentChild(upgrade, u2, u3);
    checkNotParentChild(upgrade, u3, u2);
  }


  /**
   * Standard modifies/read relationships
   */
  @Test
  public void testBasicAutomaticallyAnalyzedNodesReads() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getReadTables(U2.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getReadTables(U3.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getModifiedTables(U4.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getModifiedTables(U5.class.getName())).thenReturn(Sets.newHashSet("t1"));

    upgradeSteps.addAll(Lists.newArrayList(u1, u2, u3, u4, u5));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, u1, u2);
    checkParentChild(upgrade, u1, u3);
    checkParentChild(upgrade, u2, u4);
    checkParentChild(upgrade, u3, u4);
    checkParentChild(upgrade, u4, u5);

    checkNotParentChild(upgrade, u1, u4);
    checkNotParentChild(upgrade, u1, u5);
    checkNotParentChild(upgrade, u2, u3);
    checkNotParentChild(upgrade, u2, u5);
    checkNotParentChild(upgrade, u3, u5);
  }


  /**
   * Fallback annotations modifies/read relationships
   */
  @Test
  public void testFallbackAutomaticallyAnalyzedNodesReads() {
    // given
    when(upgradeTableResolution.getModifiedTables(FU1.class.getName())).thenReturn(Sets.newHashSet("xxx"));
    when(upgradeTableResolution.getReadTables(FU2.class.getName())).thenReturn(Sets.newHashSet("yyy"));
    when(upgradeTableResolution.getReadTables(FU3.class.getName())).thenReturn(Sets.newHashSet("zzz"));
    when(upgradeTableResolution.getModifiedTables(FU4.class.getName())).thenReturn(Sets.newHashSet("ppp"));

    upgradeSteps.addAll(Lists.newArrayList(fu1, fu2, fu3, fu4));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, fu1, fu2);
    checkParentChild(upgrade, fu1, fu3);
    checkParentChild(upgrade, fu2, fu4);
    checkParentChild(upgrade, fu3, fu4);

    checkNotParentChild(upgrade, fu1, fu4);
    checkNotParentChild(upgrade, fu2, fu3);
  }


  /**
   * ExclusiveExecution annotations
   */
  @Test
  public void testExclusiveAnnotations() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getReadTables(U2.class.getName())).thenReturn(Sets.newHashSet("t2"));
    when(upgradeTableResolution.getReadTables(EU1.class.getName())).thenReturn(Sets.newHashSet("t3"));
    when(upgradeTableResolution.getModifiedTables(EU2.class.getName())).thenReturn(Sets.newHashSet("t4"));

    upgradeSteps.addAll(Lists.newArrayList(u1, u2, eu1, eu2));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, u1, eu1);
    checkParentChild(upgrade, u2, eu1);
    checkParentChild(upgrade, eu1, eu2);

    checkNotParentChild(upgrade, u1, eu2);
    checkNotParentChild(upgrade, u2, eu2);
    checkNotParentChild(upgrade, u1, u2);
  }


  /**
   * ExclusiveExecution configuration
   */
  @Test
  public void testExclusiveConfiguration() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getReadTables(U2.class.getName())).thenReturn(Sets.newHashSet("t2"));
    when(upgradeTableResolution.getReadTables(U3.class.getName())).thenReturn(Sets.newHashSet("t3"));
    when(upgradeTableResolution.getModifiedTables(U4.class.getName())).thenReturn(Sets.newHashSet("t4"));

    upgradeSteps.addAll(Lists.newArrayList(u1, u2, u3, u4));
    exclusiveExecutionSteps.addAll(Lists.newArrayList(U3.class.getName(), U4.class.getName()));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, u1, u3);
    checkParentChild(upgrade, u2, u3);
    checkParentChild(upgrade, u3, u4);

    checkNotParentChild(upgrade, u1, u4);
    checkNotParentChild(upgrade, u2, u4);
    checkNotParentChild(upgrade, u1, u2);
  }


  /**
   * PortableSQL without fallback annotations
   */
  @Test
  public void testPortableSQLNoFallback() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getReadTables(U2.class.getName())).thenReturn(Sets.newHashSet("t2"));
    when(upgradeTableResolution.getReadTables(U3.class.getName())).thenReturn(Sets.newHashSet("t3"));
    when(upgradeTableResolution.getModifiedTables(U4.class.getName())).thenReturn(Sets.newHashSet("t4"));
    when(upgradeTableResolution.isPortableSqlStatementUsed(U3.class.getName())).thenReturn(true);
    when(upgradeTableResolution.isPortableSqlStatementUsed(U4.class.getName())).thenReturn(true);

    upgradeSteps.addAll(Lists.newArrayList(u1, u2, u3, u4));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, u1, u3);
    checkParentChild(upgrade, u2, u3);
    checkParentChild(upgrade, u3, u4);

    checkNotParentChild(upgrade, u1, u4);
    checkNotParentChild(upgrade, u2, u4);
    checkNotParentChild(upgrade, u1, u2);
  }


  /**
   * PortableSQL with fallback annotations
   */
  @Test
  public void testPortableSQLWithFallback() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getReadTables(U2.class.getName())).thenReturn(Sets.newHashSet("FT1"));
    when(upgradeTableResolution.getReadTables(FU1.class.getName())).thenReturn(Sets.newHashSet("t3"));
    when(upgradeTableResolution.getModifiedTables(FU2.class.getName())).thenReturn(Sets.newHashSet("t4"));
    when(upgradeTableResolution.isPortableSqlStatementUsed(FU1.class.getName())).thenReturn(true);
    when(upgradeTableResolution.isPortableSqlStatementUsed(FU2.class.getName())).thenReturn(true);

    upgradeSteps.addAll(Lists.newArrayList(u1, u2, fu1, fu2));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, u2, fu1);
    checkParentChild(upgrade, fu1, fu2);

    checkNotParentChild(upgrade, u1, u2);
    checkNotParentChild(upgrade, u1, fu1);
    checkNotParentChild(upgrade, u1, fu2);
  }


  /**
   * Exclusive node as a child of the root
   */
  @Test
  public void testExclusiveNodeFirst() {
    // given
    upgradeSteps.addAll(Lists.newArrayList(eu1, eu2));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, eu1, eu2);
  }


  /**
   * Standard node as a child of exclusive node
   */
  @Test
  public void testExclusiveNodeFirstThenStandard() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1000.class.getName())).thenReturn(Sets.newHashSet("t1"));
    upgradeSteps.addAll(Lists.newArrayList(eu1, eu2, u1000));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, eu1, eu2);
    checkParentChild(upgrade, eu2, u1000);

    checkNotParentChild(upgrade, eu1, u1000);
  }


  /**
   * Standard node as a child of another standard node which is a child of exlusive node
   */
  @Test
  public void testExclusiveNodeNotAParent() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1000.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getModifiedTables(U1000.class.getName())).thenReturn(Sets.newHashSet("t1", "t2"));
    upgradeSteps.addAll(Lists.newArrayList(eu1, u1000, u1001));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, eu1, u1000);
    checkParentChild(upgrade, u1000, u1001);

    checkNotParentChild(upgrade, eu1, u1001);
  }


  /**
   * Exclusive node added to leafs only
   */
  @Test
  public void testExclusiveNodeAddedAsChildToLeafsOnly() {
    // given
    when(upgradeTableResolution.getModifiedTables(U1.class.getName())).thenReturn(Sets.newHashSet("t1"));
    when(upgradeTableResolution.getModifiedTables(U2.class.getName())).thenReturn(Sets.newHashSet("t1"));
    upgradeSteps.addAll(Lists.newArrayList(eu1, u1, u2));

    // when
    GraphBasedUpgrade upgrade = builder.prepareGraphBasedUpgrade(initialisationSql);

    // then
    checkParentChild(upgrade, u1, u2);
    checkParentChild(upgrade, u2, eu1);

    checkNotParentChild(upgrade, u1, eu1);
  }


  @Test
  public void testFactory() {
    // given
    GraphBasedUpgradeBuilderFactory factory = new GraphBasedUpgradeBuilderFactory(visitorFactory, scriptGeneratorFactory, drawIOGraphPrinter);

    // when
    GraphBasedUpgradeBuilder created = factory.create(sourceSchema, targetSchema, connectionResources, exclusiveExecutionSteps, schemaChangeSequence, viewChanges);

    // then
    assertNotNull(created);
  }


  private void checkParentChild(GraphBasedUpgrade upgrade, UpgradeStep parentStep, UpgradeStep childStep) {
    GraphBasedUpgradeNode parent = findNode(upgrade, parentStep.getClass().getName());
    GraphBasedUpgradeNode child = findNode(upgrade, childStep.getClass().getName());
    assertTrue("Node: " + parentStep.getClass().getName() + " is not a parent of: " + childStep.getClass().getName(),
      parent.getChildren().contains(child));
    assertTrue("Node: " + childStep.getClass().getName() + " is not a child of: " + parentStep.getClass().getName(),
      child.getParents().contains(parent));
  }


  private void checkNotParentChild(GraphBasedUpgrade upgrade, UpgradeStep parentStep, UpgradeStep childStep) {
    GraphBasedUpgradeNode parent = findNode(upgrade, parentStep.getClass().getName());
    GraphBasedUpgradeNode child = findNode(upgrade, childStep.getClass().getName());
    assertFalse("Node: " + parentStep.getClass().getName() + " is a parent of: " + childStep.getClass().getName(),
      parent.getChildren().contains(child));
    assertFalse("Node: " + childStep.getClass().getName() + " is a child of: " + parentStep.getClass().getName(),
      child.getParents().contains(parent));
  }


  private GraphBasedUpgradeNode findNode(GraphBasedUpgrade upgrade, String name) {
    GraphBasedUpgradeNode found = findNode(upgrade.getRoot(), name, new HashSet<>());
    if(found == null) fail("Node: " + name + " couldn't be found.");
    return found;
  }

  private GraphBasedUpgradeNode findNode(GraphBasedUpgradeNode current, String name, Set<GraphBasedUpgradeNode> visited) {
    if(visited.contains(current)) return null;
    visited.add(current);

    if(current.getName().equals(name)) return current;

    for(GraphBasedUpgradeNode child : current.getChildren()) {
      GraphBasedUpgradeNode found = findNode(child, name, visited);
      if(found != null) return found;
    }

    return null;
  }

  // ---- Test Upgrade Steps

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @Sequence(1L)
  static class U1 implements UpgradeStep {

    @Override
    public String getJiraId() {
      return null;
    }

    @Override
    public String getDescription() {
      return null;
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // nevermind
    }
  }

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @Sequence(2L)
  static class U2 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @Sequence(3L)
  static class U3 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @Sequence(4L)
  static class U4 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @Sequence(5L)
  static class U5 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @UpgradeModifies("ft1")
  @Sequence(91L)
  static class FU1 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @UpgradeReads("ft1")
  @Sequence(92L)
  static class FU2 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @UpgradeReads("ft1")
  @Sequence(93L)
  static class FU3 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @UpgradeModifies("ft1")
  @Sequence(94L)
  static class FU4 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @ExclusiveExecution
  @Sequence(81L)
  static class EU1 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @ExclusiveExecution
  @Sequence(82L)
  static class EU2 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @Sequence(1000L)
  static class U1000 extends U1 {}

  /**
   * Test class.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  @Sequence(1001L)
  static class U1001 extends U1 {}
}
