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

import com.google.common.collect.ImmutableList;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaValidator;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AugmentedSchema;
import org.alfasoftware.morf.upgrade.adapt.FilteredSchema;

/**
 * A {@link SchemaChange} which consists of the addition of a new sequence to
 * a database schema.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class AddSequence implements SchemaChange {

    /** {@link org.alfasoftware.morf.metadata.Sequence} to add to the schema. */
    private final Sequence newSequence;


    /**
     * Construct an {@link AddSequence} schema change for applying against a schema.
     *
     * @param newSequence sequence to add to metadata.
     */
    public AddSequence(Sequence newSequence) {
        this.newSequence = newSequence;

        new SchemaValidator().validate(newSequence);
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
     * @return {@link Schema} with new sequence added.
     */
    @Override
    public Schema apply(Schema schema) {

        for (Sequence sequence : schema.sequences()) {
            if (sequence.getName().equals(newSequence.getName())) {
                throw new IllegalArgumentException(String.format("Cannot add sequence [%s] to schema as the sequence" +
                  " already exists", newSequence.getName()));
            }
        }

        return new AugmentedSchema(schema, newSequence);

    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
     * @return {@link Schema} with new sequence removed.
     */
    @Override
    public Schema reverse(Schema schema) {
        if(!schema.sequenceExists(newSequence.getName().toUpperCase())){
            throw new IllegalArgumentException("Cannot reverse addition of sequence [" + newSequence.getName() + "] " +
              "as it does not exist.");
        }

        return new FilteredSchema(schema, ImmutableList.of(), newSequence.getName());
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
     */
    @Override
    public boolean isApplied(Schema schema, ConnectionResources database) {
        for (String sequenceName : schema.sequenceNames()) {
            if (sequenceName.equalsIgnoreCase(newSequence.getName())) {
                return true;
            }
        }

        return false;
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
     */
    @Override
    public void accept(SchemaChangeVisitor visitor) {
        visitor.visit(this);
    }


    /**
     * @return the {@link Sequence} to be added.
     */
    public Sequence getSequence() {
        return newSequence;
    }
}
