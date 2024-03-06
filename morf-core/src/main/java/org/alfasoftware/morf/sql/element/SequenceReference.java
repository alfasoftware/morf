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

package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Class which represents a sequence in an SQL statement. Each sequence can
 * have an alias associated to it.
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public class SequenceReference extends AliasedField {

    private final String name;

    private SequenceOperation typeOfOperation;

    private SequenceReference(String alias, String name) {
        super(alias);
        this.name = name;
    }

    /**
     * Construct a new sequence with a given name.
     *
     * @param name the name of the sequence
     */
    public SequenceReference(String name) {
        this("", name);
    }


    /**
     * Get the name of the sequence
     *
     * @return the name
     */
    public String getName() {
        return name;
    }


    /**
     * Get the operation type for the sequence
     *
     * @return the name
     */
    public SequenceOperation getTypeOfOperation() {
        return typeOfOperation;
    }


    /**
     * Sets the operation type of the sequence to return the next value
     * @return the {@link SequenceReference}
     */
    public SequenceReference nextValue() {
        this.typeOfOperation = SequenceOperation.NEXT_VALUE;
        return this;
    }

    /**
     * Sets the operation type of the sequence to return the current value
     * @return the {@link SequenceReference}
     */
    public SequenceReference currentValue() {
        this.typeOfOperation = SequenceOperation.CURRENT_VALUE;
        return this;
    }


    @Override
    public void accept(SchemaAndDataChangeVisitor visitor) {
        visitor.visit(this);
    }


    /**
     * {@inheritDoc}
     * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
     */
    @Override
    protected AliasedFieldBuilder deepCopyInternal(DeepCopyTransformation transformer) {
        return new SequenceReference(getAlias(), name);
    }


    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SequenceReference other = (SequenceReference) obj;
        return new EqualsBuilder()
            .appendSuper(super.equals(obj))
            .append(typeOfOperation, other.typeOfOperation)
            .append(name, other.name)
            .isEquals();
    }


    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .appendSuper(super.hashCode())
            .append(typeOfOperation)
            .append(name)
            .build();
    }

}
