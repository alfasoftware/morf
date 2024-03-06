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
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.upgrade.adapt.AugmentedSchema;
import org.alfasoftware.morf.upgrade.adapt.FilteredSchema;


/**
 * A {@link SchemaChange} which consists of the removal of a new sequence to
 * a database schema.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class RemoveSequence implements SchemaChange {

    /** The {@link Sequence} to remove from the schema. */
    private final Sequence sequenceToBeRemoved;

    /**
     * Construct a {@link RemoveSequence} schema change for applying against a schema.
     *
     * @param sequenceToBeRemoved the sequence to be removed from schema
     */
    public RemoveSequence(Sequence sequenceToBeRemoved) {
        this.sequenceToBeRemoved = sequenceToBeRemoved;
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
     * @return {@link Schema} with sequence removed.
     */
    @Override
    public Schema apply(Schema schema) {
        if(!schema.sequenceExists(sequenceToBeRemoved.getName().toUpperCase())){
            throw new IllegalArgumentException("Cannot remove sequence [" + sequenceToBeRemoved.getName() + "] as it " +
                "does not exist.");
        }

        return new FilteredSchema(schema, ImmutableList.of(), sequenceToBeRemoved.getName());
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
     * @return {@link Schema} with sequence added back in.
     */
    @Override
    public Schema reverse(Schema schema) {
        if(schema.sequenceExists(sequenceToBeRemoved.getName().toUpperCase())){
            throw new IllegalArgumentException("Cannot perform reversal for [" + sequenceToBeRemoved.getName() + "] " +
                "sequence removal as it already exists.");
        }

        return new AugmentedSchema(schema, sequenceToBeRemoved);
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
     */
    @Override
    public boolean isApplied(Schema schema, ConnectionResources database) {
        for (String sequenceName : schema.sequenceNames()) {
            if (sequenceName.equalsIgnoreCase(sequenceToBeRemoved.getName())) {
                return false;
            }
        }

        return true;
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
        return sequenceToBeRemoved;
    }
}
