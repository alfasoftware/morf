package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;

import java.util.Collection;

import org.alfasoftware.morf.jdbc.postgresql.PostgreSQLUniqueIndexAdditionalDeploymentStatements.AdditionalIndexInfo;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestPostgreSQLUniqueIndexAdditionalDeploymentStatements {

  private static final String SCHEMA_PREFIX = "public.";
  private static final String TABLE_NAME = "TableName";

  private final Index index1 = index("TableName_1").columns("u").unique();
  private final Index index2 = index("TableName_2").columns("a", "u").unique();
  private final Index index3 = index("TableName_3").columns("a", "b", "c").unique();
  private final Index index4 = index("TableName_4").columns("a", "b", "u").unique();
  private final Index index5 = index("TableName_5").columns("u", "b", "v").unique();
  private final Index index6 = index("TableName_6").columns("u", "v", "x").unique();

  private final Table table =
      table(TABLE_NAME)
        .columns(
          column("id", DataType.BIG_INTEGER),
          column("a", DataType.BIG_INTEGER),
          column("b", DataType.BIG_INTEGER),
          column("c", DataType.BIG_INTEGER),
          column("u", DataType.BIG_INTEGER).nullable(),
          column("v", DataType.BIG_INTEGER).nullable(),
          column("x", DataType.BIG_INTEGER).nullable())
        .indexes(index1, index2, index3, index4, index5, index6);

  @Test
  public void testCreateIndexStatementsForIndex1() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index1).createIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_1$null0 ON public.TableName ((0)) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_1$null0 IS 'REALNAME:[6285d92226e5112908a10cd51e129b72/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }

  @Test
  public void testCreateIndexStatementsForIndex2() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index2).createIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_2$null0 ON public.TableName (a) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_2$null0 IS 'REALNAME:[9cbb012537901e9f3520ae3fcf7dc43d/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }

  @Test
  public void testCreateIndexStatementsForIndex3() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index3).createIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      emptyIterable());
  }

  @Test
  public void testCreateIndexStatementsForIndex4() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index4).createIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_4$null0 ON public.TableName (a, b) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_4$null0 IS 'REALNAME:[788721e3ce858194769fdf67aab7b14c/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }

  @Test
  public void testCreateIndexStatementsForIndex5() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index5).createIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_5$null0 ON public.TableName (b, v) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_5$null0 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/cfcd208495d565ef66e7dff9f98764da]'",
        "CREATE UNIQUE INDEX TableName_5$null01 ON public.TableName (b) WHERE u IS NULL AND v IS NULL",
        "COMMENT ON INDEX TableName_5$null01 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/ae31e892f55b0e0225acd2c08fa2c413]'",
        "CREATE UNIQUE INDEX TableName_5$null1 ON public.TableName (u, b) WHERE v IS NULL",
        "COMMENT ON INDEX TableName_5$null1 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/c4ca4238a0b923820dcc509a6f75849b]'"
      ));
  }

  @Test
  public void testCreateIndexStatementsForIndex6() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index6).createIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_6$null ON public.TableName ((u ||'§'|| v ||'§'|| x)) WHERE u IS NULL OR v IS NULL OR x IS NULL",
        "COMMENT ON INDEX TableName_6$null IS 'REALNAME:[47d0d3041096c2ac2e8cbb4a8d0e3fd8]'"
      ));
  }


  @Test
  public void renameIndexStatementsForIndex1() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index1).renameIndexStatements(SCHEMA_PREFIX + "TableName_1", "TableName_1a"),
      contains(
        "ALTER INDEX IF EXISTS public.TableName_1$null0 RENAME TO TableName_1a$null0"
      ));
  }

  @Test
  public void renameIndexStatementsForIndex2() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index2).renameIndexStatements(SCHEMA_PREFIX + "TableName_2", "TableName_2a"),
      contains(
        "ALTER INDEX IF EXISTS public.TableName_2$null0 RENAME TO TableName_2a$null0"
      ));
  }

  @Test
  public void renameIndexStatementsForIndex3() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index3).renameIndexStatements(SCHEMA_PREFIX + "TableName_3", "TableName_3a"),
      emptyIterable());
  }

  @Test
  public void renameIndexStatementsForIndex4() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index4).renameIndexStatements(SCHEMA_PREFIX + "TableName_4", "TableName_4a"),
      contains(
        "ALTER INDEX IF EXISTS public.TableName_4$null0 RENAME TO TableName_4a$null0"
      ));
  }

  @Test
  public void renameIndexStatementsForIndex5() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index5).renameIndexStatements(SCHEMA_PREFIX + "TableName_5", "TableName_5a"),
      contains(
        "ALTER INDEX IF EXISTS public.TableName_5$null0 RENAME TO TableName_5a$null0",
        "ALTER INDEX IF EXISTS public.TableName_5$null01 RENAME TO TableName_5a$null01",
        "ALTER INDEX IF EXISTS public.TableName_5$null1 RENAME TO TableName_5a$null1"
      ));
  }

  @Test
  public void renameIndexStatementsForIndex6() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index6).renameIndexStatements(SCHEMA_PREFIX + "TableName_6", "TableName_6a"),
      contains(
        "ALTER INDEX IF EXISTS public.TableName_6$null RENAME TO TableName_6a$null"
      ));
  }


  @Test
  public void testDropIndexStatementsForIndex1() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index1).dropIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS TableName_1$null0"
      ));
  }

  @Test
  public void testDropIndexStatementsForIndex2() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index2).dropIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS TableName_2$null0"
      ));
  }

  @Test
  public void testDropIndexStatementsForIndex3() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index3).dropIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      emptyIterable());
  }

  @Test
  public void testDropIndexStatementsForIndex4() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index4).dropIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS TableName_4$null0"
      ));
  }

  @Test
  public void testDropIndexStatementsForIndex5() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index5).dropIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS TableName_5$null0",
        "DROP INDEX IF EXISTS TableName_5$null01",
        "DROP INDEX IF EXISTS TableName_5$null1"
      ));
  }

  @Test
  public void testDropIndexStatementsForIndex6() {
    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index6).dropIndexStatements(SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS TableName_6$null"
      ));
  }


  @Test
  public void testHealIndexStatementsForIndex1FromNoIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of();

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index1).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_1$null0 ON public.TableName ((0)) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_1$null0 IS 'REALNAME:[6285d92226e5112908a10cd51e129b72/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }

  @Test
  public void testHealIndexStatementsForIndex1FromGoodIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_1$null0", "6285d92226e5112908a10cd51e129b72/cfcd208495d565ef66e7dff9f98764da").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index1).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      emptyIterable());
  }

  @Test
  public void testHealIndexStatementsForIndex1FromWrongHash() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_1$null0", "6285d92226e5112908a10cd51e129b72/xx").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index1).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_1$null0",
        "CREATE UNIQUE INDEX TableName_1$null0 ON public.TableName ((0)) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_1$null0 IS 'REALNAME:[6285d92226e5112908a10cd51e129b72/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }

  @Test
  public void testHealIndexStatementsForIndex1FromWrongName() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_1$null1", "6285d92226e5112908a10cd51e129b72/cfcd208495d565ef66e7dff9f98764da").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index1).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_1$null1",
        "CREATE UNIQUE INDEX TableName_1$null0 ON public.TableName ((0)) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_1$null0 IS 'REALNAME:[6285d92226e5112908a10cd51e129b72/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }


  @Test
  public void testHealIndexStatementsForIndex2FromNoIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of();

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index2).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_2$null0 ON public.TableName (a) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_2$null0 IS 'REALNAME:[9cbb012537901e9f3520ae3fcf7dc43d/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }

  @Test
  public void testHealIndexStatementsForIndex2FromGoodIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_2$null0", "9cbb012537901e9f3520ae3fcf7dc43d/cfcd208495d565ef66e7dff9f98764da").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index2).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      emptyIterable());
  }


  @Test
  public void testHealIndexStatementsForIndex2FromWrongHash() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_2$null0", "xx/cfcd208495d565ef66e7dff9f98764da").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index2).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_2$null0",
        "CREATE UNIQUE INDEX TableName_2$null0 ON public.TableName (a) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_2$null0 IS 'REALNAME:[9cbb012537901e9f3520ae3fcf7dc43d/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }


  @Test
  public void testHealIndexStatementsForIndex2FromWrongName() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_2$null", "9cbb012537901e9f3520ae3fcf7dc43d/cfcd208495d565ef66e7dff9f98764da").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index2).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_2$null",
        "CREATE UNIQUE INDEX TableName_2$null0 ON public.TableName (a) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_2$null0 IS 'REALNAME:[9cbb012537901e9f3520ae3fcf7dc43d/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }


  @Test
  public void testHealIndexStatementsForIndex3FromNoIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of();

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index3).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      emptyIterable());
  }


  @Test
  public void testHealIndexStatementsForIndex3FromWrongIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_3$null", "xyz/abc").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index3).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_3$null"
      ));
  }


  @Test
  public void testHealIndexStatementsForIndex4FromNoIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of();

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index4).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_4$null0 ON public.TableName (a, b) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_4$null0 IS 'REALNAME:[788721e3ce858194769fdf67aab7b14c/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }

  @Test
  public void testHealIndexStatementsForIndex4FromGoodIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_4$null0", "788721e3ce858194769fdf67aab7b14c/cfcd208495d565ef66e7dff9f98764da").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index4).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      emptyIterable());
  }

  @Test
  public void testHealIndexStatementsForIndex4FromWrongHash() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_4$null0", "788721e3ce858194769fdf67aab7b14c/xx").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index4).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_4$null0",
        "CREATE UNIQUE INDEX TableName_4$null0 ON public.TableName (a, b) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_4$null0 IS 'REALNAME:[788721e3ce858194769fdf67aab7b14c/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }

  @Test
  public void testHealIndexStatementsForIndex4FromWrongName() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_4$null4", "788721e3ce858194769fdf67aab7b14c/cfcd208495d565ef66e7dff9f98764da").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index4).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_4$null4",
        "CREATE UNIQUE INDEX TableName_4$null0 ON public.TableName (a, b) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_4$null0 IS 'REALNAME:[788721e3ce858194769fdf67aab7b14c/cfcd208495d565ef66e7dff9f98764da]'"
      ));
  }


  @Test
  public void testHealIndexStatementsForIndex5FromNoIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of();

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index5).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_5$null0 ON public.TableName (b, v) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_5$null0 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/cfcd208495d565ef66e7dff9f98764da]'",
        "CREATE UNIQUE INDEX TableName_5$null01 ON public.TableName (b) WHERE u IS NULL AND v IS NULL",
        "COMMENT ON INDEX TableName_5$null01 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/ae31e892f55b0e0225acd2c08fa2c413]'",
        "CREATE UNIQUE INDEX TableName_5$null1 ON public.TableName (u, b) WHERE v IS NULL",
        "COMMENT ON INDEX TableName_5$null1 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/c4ca4238a0b923820dcc509a6f75849b]'"
      ));
  }

  @Test
  public void testHealIndexStatementsForIndex5FromGoodIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null0", "a42e2a14d8ae857335a014ab1a1f1ad7/cfcd208495d565ef66e7dff9f98764da").get(),
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null01", "a42e2a14d8ae857335a014ab1a1f1ad7/ae31e892f55b0e0225acd2c08fa2c413").get(),
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null01", "a42e2a14d8ae857335a014ab1a1f1ad7/ae31e892f55b0e0225acd2c08fa2c413").get(), // duplicates are supported
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null1", "a42e2a14d8ae857335a014ab1a1f1ad7/c4ca4238a0b923820dcc509a6f75849b").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index5).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      emptyIterable());
  }

  @Test
  public void testHealIndexStatementsForIndex5FromWrongHash() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null0", "a42e2a14d8ae857335a014ab1a1f1ad7/cfcd208495d565ef66e7dff9f98764da").get(),
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null01", "a42e2a14d8ae857335a014ab1a1f1ad7/xxxx").get(), // wrong hash
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null1", "a42e2a14d8ae857335a014ab1a1f1ad7/c4ca4238a0b923820dcc509a6f75849b").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index5).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_5$null0",
        "DROP INDEX IF EXISTS tablename_5$null01",
        "DROP INDEX IF EXISTS tablename_5$null1",
        "CREATE UNIQUE INDEX TableName_5$null0 ON public.TableName (b, v) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_5$null0 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/cfcd208495d565ef66e7dff9f98764da]'",
        "CREATE UNIQUE INDEX TableName_5$null01 ON public.TableName (b) WHERE u IS NULL AND v IS NULL",
        "COMMENT ON INDEX TableName_5$null01 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/ae31e892f55b0e0225acd2c08fa2c413]'",
        "CREATE UNIQUE INDEX TableName_5$null1 ON public.TableName (u, b) WHERE v IS NULL",
        "COMMENT ON INDEX TableName_5$null1 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/c4ca4238a0b923820dcc509a6f75849b]'"
      ));
  }

  @Test
  public void testHealIndexStatementsForIndex5FromWrongName() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null0", "a42e2a14d8ae857335a014ab1a1f1ad7/cfcd208495d565ef66e7dff9f98764da").get(),
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null", "a42e2a14d8ae857335a014ab1a1f1ad7/ae31e892f55b0e0225acd2c08fa2c413").get(), // wrong name
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_5$null1", "a42e2a14d8ae857335a014ab1a1f1ad7/c4ca4238a0b923820dcc509a6f75849b").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index5).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_5$null0",
        "DROP INDEX IF EXISTS tablename_5$null",
        "DROP INDEX IF EXISTS tablename_5$null1",
        "CREATE UNIQUE INDEX TableName_5$null0 ON public.TableName (b, v) WHERE u IS NULL",
        "COMMENT ON INDEX TableName_5$null0 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/cfcd208495d565ef66e7dff9f98764da]'",
        "CREATE UNIQUE INDEX TableName_5$null01 ON public.TableName (b) WHERE u IS NULL AND v IS NULL",
        "COMMENT ON INDEX TableName_5$null01 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/ae31e892f55b0e0225acd2c08fa2c413]'",
        "CREATE UNIQUE INDEX TableName_5$null1 ON public.TableName (u, b) WHERE v IS NULL",
        "COMMENT ON INDEX TableName_5$null1 IS 'REALNAME:[a42e2a14d8ae857335a014ab1a1f1ad7/c4ca4238a0b923820dcc509a6f75849b]'"
      ));
  }


  @Test
  public void testHealIndexStatementsForIndex6FromNoIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of();

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index6).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "CREATE UNIQUE INDEX TableName_6$null ON public.TableName ((u ||'§'|| v ||'§'|| x)) WHERE u IS NULL OR v IS NULL OR x IS NULL",
        "COMMENT ON INDEX TableName_6$null IS 'REALNAME:[47d0d3041096c2ac2e8cbb4a8d0e3fd8]'"
    ));
  }

  @Test
  public void testHealIndexStatementsForIndex6FromGoodIndexes() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_6$null", "47d0d3041096c2ac2e8cbb4a8d0e3fd8").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index6).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      emptyIterable());
  }

  @Test
  public void testHealIndexStatementsForIndex6FromWrongHash() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_6$null", "xx").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index6).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_6$null",
        "CREATE UNIQUE INDEX TableName_6$null ON public.TableName ((u ||'§'|| v ||'§'|| x)) WHERE u IS NULL OR v IS NULL OR x IS NULL",
        "COMMENT ON INDEX TableName_6$null IS 'REALNAME:[47d0d3041096c2ac2e8cbb4a8d0e3fd8]'"
      ));
  }

  @Test
  public void testHealIndexStatementsForIndex6FromWrongName() {
    final Collection<AdditionalIndexInfo> additionalConstraintIndexInfos = ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableName_6$null6", "47d0d3041096c2ac2e8cbb4a8d0e3fd8").get()
    );

    assertThat(
      new PostgreSQLUniqueIndexAdditionalDeploymentStatements(table, index6).healIndexStatements(additionalConstraintIndexInfos, SCHEMA_PREFIX + TABLE_NAME),
      contains(
        "DROP INDEX IF EXISTS tablename_6$null6",
        "CREATE UNIQUE INDEX TableName_6$null ON public.TableName ((u ||'§'|| v ||'§'|| x)) WHERE u IS NULL OR v IS NULL OR x IS NULL",
        "COMMENT ON INDEX TableName_6$null IS 'REALNAME:[47d0d3041096c2ac2e8cbb4a8d0e3fd8]'"
      ));
  }
}
