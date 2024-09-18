/* Copyright 2024 Alfa Financial Software
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

package org.alfasoftware.morf.sql.element;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Portable databaseFunctionMap allows for SQL databaseFunctionMap to be freely defined using the following syntax:
 * <p>functionName(argument1, argument2...)</p>
 * <p>Function definitions are mapped to database type identifiers, and distinct statements are required for each supported database</p>
 * <p>These portable functions should only be used when it is absolutely necessary, for instance when a specific function
 * is required where there is no support (nor planned support) within morf.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public final class PortableFunction extends AliasedField {

  private final Map<String, Pair<String, List<AliasedField>>> databaseFunctionMap;

  /**
   * Convenience method to build a portable function for a single database type. May be used with {@link PortableFunction#addFunctionForDatabaseType} to define functions
   * for further database types
   *
   * @param databaseTypeIdentifier this identifier should match the DatabaseType.IDENTIFIER value
   * @param functionName the name of the function to be generated
   * @param arguments the arguments for the function
   * @return the portable function to be used
   */
  public static PortableFunction withFunctionForDatabaseType(String databaseTypeIdentifier, String functionName, AliasedField... arguments) {
    Pair<String, List<AliasedField>> functionWithArguments = Pair.of(functionName, Arrays.asList(arguments));
    Map<String, Pair<String, List<AliasedField>>> databaseFunctionMap = new HashMap<>();

    databaseFunctionMap.put(databaseTypeIdentifier, functionWithArguments);
    return new PortableFunction(databaseFunctionMap);
  }


  /**
   * Can be used to add functions for further database types, once the PortableFunction has been built using {@link PortableFunction#withFunctionForDatabaseType}
   *
   * @param databaseTypeIdentifier this identifier should match the DatabaseType.IDENTIFIER value
   * @param functionName the name of the function to be generated
   * @param arguments the arguments for the function
   * @return the portable function to be used
   */
  public PortableFunction addFunctionForDatabaseType(String databaseTypeIdentifier, String functionName, AliasedField... arguments) {
    Pair<String, List<AliasedField>> functionWithArguments = Pair.of(functionName, Arrays.asList(arguments));

    databaseFunctionMap.put(databaseTypeIdentifier, functionWithArguments);
    return this;
  }


  /**
   * @return the function and arguments for the provided databaseTypeIdentifier. Throws an UnsupportedOperationException
   * if no function is found.
   */
  public Pair<String, List<AliasedField>> getFunctionForDatabaseType(String databaseTypeIdentifier) {
    Pair<String, List<AliasedField>> functionWithArguments = databaseFunctionMap.get(databaseTypeIdentifier);

    if (functionWithArguments == null) {
      throw new UnsupportedOperationException("Portable statement not found for database type: " + databaseTypeIdentifier);
    }

    return functionWithArguments;
  }


  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
  }


  @Override
  protected PortableFunction deepCopyInternal(DeepCopyTransformation transformer) {
    return new PortableFunction(PortableFunction.this);
  }


  private PortableFunction(Map<String, Pair<String, List<AliasedField>>> functions) {
    this.databaseFunctionMap = functions;
  }


  private PortableFunction(PortableFunction sourceFunction) {
    super(sourceFunction.getAlias());
    this.databaseFunctionMap = sourceFunction.databaseFunctionMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> Pair.of(e.getValue().getLeft(), List.copyOf(e.getValue().getRight()))));

  }
}