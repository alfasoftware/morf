package org.alfasoftware.morf.metadata;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public abstract class PartitioningByRangeRule<T,R> implements PartitioningRule {
    protected final String column;
    protected final T startValue;
    protected final R increment;
    protected final int count;

    public PartitioningByRangeRule(String column, T startValue, R increment, int count) {
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }
        this.column = column;
        this.startValue = startValue;
        this.increment = increment;
        this.count = count;
    }

    @Override
    public String getColumn() { return column; }

    abstract protected List<Pair<T, T>> getRanges();
}
