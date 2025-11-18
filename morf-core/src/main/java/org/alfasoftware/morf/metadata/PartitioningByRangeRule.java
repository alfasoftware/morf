package org.alfasoftware.morf.metadata;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

public abstract class PartitioningByRangeRule<T,R> implements PartitioningRule {
    protected final String column;
    protected final T startValue;
    protected final R increment;
    protected final int count;
    protected List<Pair<T, T>> partitions;
    protected DataType columnType;

    protected abstract List<Pair<T, T>> getRanges();

    public PartitioningByRangeRule(String column, DataType columnType, T startValue, R increment, int count) {
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }
        this.column = column;
        this.startValue = startValue;
        this.increment = increment;
        this.count = count;
        this.partitions = getRanges();
        this.columnType = columnType;
    }


    public PartitioningByRangeRule(String column, DataType columnType, List<Pair<T, T>> ranges) {
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }
        this.column = column;
        this.startValue = null;
        this.increment = null;
        this.count = ranges.size();
        this.partitions = ranges;
        this.columnType = columnType;
    }


    @Override
    public DataType getColumnType() {
        return columnType;
    }


    @Override
    public String getColumn() { return column; }

    @Override
    public PartitioningRuleType getPartitioningType() { return PartitioningRuleType.rangePartitioning; }
}
