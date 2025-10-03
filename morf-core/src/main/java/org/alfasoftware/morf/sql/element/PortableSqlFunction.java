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
import java.util.Objects;
import java.util.stream.Collectors;

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Portable functions allow for SQL functions to be freely defined using the following syntax:
 * <p>functionName(argument1, argument2...)</p>
 * <p>Function definitions are mapped to database type identifiers, and distinct statements are required for each supported database</p>
 * <p>These portable functions should only be used when it is absolutely necessary, for instance when a specific function
 * is required where there is no support (nor planned support) within morf.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public final class PortableSqlFunction extends AliasedField {

    private Map<String, Pair<String, List<AliasedField>>> databaseFunctionMap = new HashMap<>();

    /**
     * Starts a new PortableSqlFunction builder
     */
    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder implements AliasedFieldBuilder {

        private final Map<String, Pair<String, List<AliasedField>>> databaseFunctionMap = new HashMap<>();
        private String alias;

        /**
         * Adds a portable function for a single database type.
         *
         * @param databaseTypeIdentifier this identifier should match the DatabaseType.IDENTIFIER value
         * @param functionName           the name of the function to be generated
         * @param arguments              the arguments for the function
         * @return the builder
         */
        public Builder withFunctionForDatabaseType(String databaseTypeIdentifier, String functionName, AliasedField... arguments) {
            Pair<String, List<AliasedField>> functionWithArguments = Pair.of(functionName, Arrays.asList(arguments));
            databaseFunctionMap.put(databaseTypeIdentifier, functionWithArguments);

            return this;
        }

        @Override
        public Builder as(String alias) {
            this.alias = alias;
            return this;
        }

        public PortableSqlFunction build() {
            return new PortableSqlFunction(databaseFunctionMap, alias);
        }
    }


    /**
     * @return the function and arguments for the provided databaseTypeIdentifier. Throws an UnsupportedOperationException
     * if no function is found.
     */
    public Pair<String, List<AliasedField>> getFunctionForDatabaseType(String databaseTypeIdentifier) {
        Pair<String, List<AliasedField>> functionWithArguments = databaseFunctionMap.get(databaseTypeIdentifier);

        if (functionWithArguments == null) {
            throw new UnsupportedOperationException("Portable function not found for database type: " + databaseTypeIdentifier);
        }

        return functionWithArguments;
    }


    private PortableSqlFunction(Map<String, Pair<String, List<AliasedField>>> functions, String alias) {
        super(alias);
        this.databaseFunctionMap = functions;
    }


    private PortableSqlFunction(PortableSqlFunction sourceFunction) {
        super(sourceFunction.getAlias());
        this.databaseFunctionMap = sourceFunction.databaseFunctionMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> Pair.of(e.getValue().getLeft(), List.copyOf(e.getValue().getRight()))));
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PortableSqlFunction)) return false;
        if (!super.equals(o)) return false;
        PortableSqlFunction that = (PortableSqlFunction) o;
        return Objects.equals(databaseFunctionMap, that.databaseFunctionMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), databaseFunctionMap);
    }

    @Override
    public void accept(SchemaAndDataChangeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    protected PortableSqlFunction deepCopyInternal(DeepCopyTransformation transformer) {
        return new PortableSqlFunction(PortableSqlFunction.this);
    }
}