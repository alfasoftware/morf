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

package org.alfasoftware.morf.metadata;


/**
 * Implements {@linkplain Sequence} as a bean
 *
 * @author Copyright (c) Alfa Financial Software 2024 Ltd.
 */
public class SequenceBean implements Sequence {

    private final String name;

    private final Integer startsWith;

    private final boolean isTemporary;


    /**
     * Create a new sequence
     *
     * @param name The sequence name
     * @param startsWith The integer tha the sequence starts with
     * @param isTemporary true if the boolean is temporary
     */
    SequenceBean(String name, Integer startsWith, boolean isTemporary) {
        super();
        this.name = name;
        this.startsWith = startsWith;
        this.isTemporary = isTemporary;
    }


    /**
     * Copy an existing {@link Sequence}.
     *
     * @param sequence The sequence to copy.
     */
    SequenceBean(Sequence sequence) {
        super();
        this.name = sequence.getName();
        this.startsWith = sequence.getStartsWith() != null ? sequence.getStartsWith() : null;
        this.isTemporary = sequence.isTemporary();
    }

    /**
     * @see Sequence#getName()
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * @see Sequence#getStartsWith()
     */
    @Override
    public Integer getStartsWith() {
        return startsWith;
    }

    /**
     * @see Sequence#isTemporary()
     */
    @Override
    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public String toString() {
        return "Sequence-" + getName();
    }
}
