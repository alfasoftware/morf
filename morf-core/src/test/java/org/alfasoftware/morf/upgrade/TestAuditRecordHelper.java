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

package org.alfasoftware.morf.upgrade;

import static com.google.common.collect.FluentIterable.from;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FunctionType;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link AuditRecordHelper}.
 */
public class TestAuditRecordHelper {

  /**
   * Tests the format of the upgrade audit record.
   * @throws ParseException If the date on the record cannot be parsed.
   */
  @Test
  public void testAddAuditRecord() throws ParseException {
    // given
    SchemaChangeVisitor visitor = mock(SchemaChangeVisitor.class);
    Schema schema = mock(Schema.class);
    Table table = mock(Table.class);
    List<Column> columns = Arrays.asList(mock(Column.class), mock(Column.class), mock(Column.class), mock(Column.class), mock(Column.class), mock(Column.class), mock(Column.class));

    UUID uuid = UUID.randomUUID();
    String description = "Description";
    given(schema.tableExists("UpgradeAudit")).willReturn(true);
    given(schema.getTable("UpgradeAudit")).willReturn(table);
    given(table.columns()).willReturn(columns);

    // when
    AuditRecordHelper.addAuditRecord(visitor, schema, uuid, description);

    // then
    ArgumentCaptor<ExecuteStatement> argument = ArgumentCaptor.forClass(ExecuteStatement.class);
    verify(visitor).visit(argument.capture());
    InsertStatement statement = (InsertStatement) argument.getValue().getStatement();
    assertAuditInsertStatement(uuid, description, statement);
  }


  /**
   * Tests the upgrade audit record is not created if the UpgradeAudit table is still in the old 3 column format
   */
  @Test
  public void testAddAuditRecordPreExpansion() {
    // given
    SchemaChangeVisitor visitor = mock(SchemaChangeVisitor.class);
    Schema schema = mock(Schema.class);
    Table table = mock(Table.class);
    List<Column> columns = Arrays.asList(mock(Column.class), mock(Column.class), mock(Column.class));

    UUID uuid = UUID.randomUUID();
    String description = "Description";
    given(schema.tableExists("UpgradeAudit")).willReturn(true);
    given(schema.getTable("UpgradeAudit")).willReturn(table);
    given(table.columns()).willReturn(columns);

    // when
    AuditRecordHelper.addAuditRecord(visitor, schema, uuid, description);

    // then
    ArgumentCaptor<ExecuteStatement> argument = ArgumentCaptor.forClass(ExecuteStatement.class);
    assert(argument.getAllValues().size()==0);
  }


  /**
   * Verifies that the {@link AuditRecordHelper#createAuditInsertStatement(UUID, String)} returns a correct
   * {@link InsertStatement}.
   */
  @Test
  public void createAuditInsertStatement() throws Exception {
    // given
    UUID uuid = UUID.randomUUID();
    String description = "Description";

    // when
    InsertStatement statement = AuditRecordHelper.createAuditInsertStatement(uuid, description);

    // then
    assertAuditInsertStatement(uuid, description, statement);
  }



  /**
   * Verifies that the {@link AuditRecordHelper#createAuditInsertStatement(UUID, String)} returns a correct
   * {@link InsertStatement}.
   */
  @Test
  public void createAuditInsertStatementLongDescription() throws Exception {
    // given
    UUID uuid = UUID.randomUUID();
    String str10 = "0123456789";
    String str40 = str10 + str10 + str10 + str10;
    String str200 = str40 + str40 + str40 + str40 + str40;
    String str210 = str200 + str10;

    // when an overlength description is passed
    InsertStatement statement = AuditRecordHelper.createAuditInsertStatement(uuid, str210);

    // then it is trimmed to 200 characters
    assertAuditInsertStatement(uuid, str200, statement);
  }





  private void assertAuditInsertStatement(UUID uuid, String description, InsertStatement statement) {
    assertEquals("Table name", "UpgradeAudit", statement.getTable().getName());
    assertEquals("UUID ", uuid.toString(), getValueWithAlias(statement, "upgradeUUID").getValue());
    assertEquals("UUID ", description, getValueWithAlias(statement, "description").getValue());

    Cast nowCastRepresentation = getCastWithAlias(statement, "appliedTime");
    assertEquals("Wrapped in integer date function with now function as argument", FunctionType.DATE_TO_YYYYMMDDHHMMSS + "(" + FunctionType.NOW + "())", nowCastRepresentation.getExpression().toString());
  }


  private static FieldLiteral getValueWithAlias(InsertStatement statement, String alias) {
    for (AliasedField aliasedField : statement.getValues())
      if (aliasedField.getAlias().equals(alias))
        return (FieldLiteral) aliasedField;
    throw new NoSuchElementException("No field with alias " + alias + " found in supplied values.");
  }


  private static Cast getCastWithAlias(InsertStatement statement, final String alias) {
    return from(statement.getValues()).filter(Cast.class)
        .filter(input -> StringUtils.equals(alias, input.getAlias()))
        .first().get();
  }

}
