package org.alfasoftware.morf.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.joda.time.ReadablePeriod;

public class DatePartitionedByPeriodRule extends PartitioningByRangeRule<LocalDate, Period> {

    public DatePartitionedByPeriodRule(String column, LocalDate startValue, Period period, int count) {
        super(column, startValue, period, count);
    }

    @Override
    public List<Pair<LocalDate, LocalDate>> getRanges() {
        List<Pair<LocalDate, LocalDate>> ranges = new ArrayList<Pair<LocalDate, LocalDate>>();

        ReadablePeriod readablePeriod = increment.toPeriod();
        startValue.plus(readablePeriod);

        int i = count;
        for (LocalDate current = startValue; i > 0; i--) {
            ranges.add(Pair.of(current, current.plus(readablePeriod)));
            current = current.plus(readablePeriod);
        }

        return ranges;
    }
}
