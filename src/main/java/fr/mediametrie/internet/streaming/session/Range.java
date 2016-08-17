package fr.mediametrie.internet.streaming.session;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;

import fr.mediametrie.internet.streaming.util.NumberUtils;

/**
 * Simple immutable Range with lower and upper bounds.
 */
public class Range implements Serializable, Comparable<Range> {
    private final long lowerBound;
    private final long upperBound;

    public Range(long lower, long upper) {
        this.lowerBound = lower;
        this.upperBound = upper;
    }

    /**
     * @return a new range with a single value as lower and upper.
     */
    public static Range single(long value) {
        return new Range(value, value);
    }

    /**
     * Build a new {@link Range} with the given bounds. The lower bound will be the smallest value and the upper bound
     * will be the higher.
     */
    public static Range range(long bound1, long bound2) {
        return bound1 <= bound2 ? new Range(bound1, bound2) : new Range(bound2, bound1);
    }

    /**
     * @return if the given range overlaps this range (given range lowerbound between this range
     *         lower an upper bounds).
     */
    public boolean overlapWith(@Nonnull Range range) {
        return range.lowerBound >= this.lowerBound && range.lowerBound <= this.upperBound;
    }

    /**
     * Build an ordered set of {@link Range} from the given values.
     * Values are grouped in the same range when the difference between each value and its direct higher value is less
     * or equal to the step value. i.e: with a step set to 5, values 2,5,10,12 will be grouped whereas 18,22,25
     * will be grouped in another range.
     */
    public static TreeSet<Range> build(@Nonnull Collection<Long> values, int step) {
        TreeSet<Long> orderedValues = new TreeSet<>(values);
        TreeSet<Range> ranges = new TreeSet<>();
        Long lower = null;
        Long upper = null;
        for (Long value : orderedValues) {
            if (lower == null) {
                lower = value;
                upper = value;
            } else {
                if (value - upper > step) {
                    ranges.add(Range.range(lower, upper));
                    lower = value;
                    upper = value;
                } else {
                    upper = value;
                }
            }
        }
        if (lower != null) {
            ranges.add(Range.range(lower, upper));
        }
        return ranges;
    }

    /**
     * Flat all ranges in a array of values from lower bound to upper bound (both rounded down to step)
     * increasing by step.
     * Note: if the ranges are not merged, the resulting array could contain duplicates.
     */
    public static long[] flatRangesWithStep(@Nonnull TreeSet<Range> ranges, int step) {
        return ranges.stream().flatMapToLong(range -> range.flatToLongStream(step)).toArray();
    }

    /**
     * Flat this range in a array of values from lower bound to upper bound (both rounded down to step)
     * increasing by step.
     */
    protected LongStream flatToLongStream(int step) {
        return StreamSupport.stream(new StepIterable(step).spliterator(), false).mapToLong(Long::valueOf);
    }

    /**
     * Flat this range in a array of values from lower bound to upper bound (both rounded down to step)
     * increasing by step.
     */
    public long[] flat(int step) {
        return flatToLongStream(step).toArray();
    }

    @Override
    public int compareTo(@Nonnull Range other) {
        int lowerCompareResult = Long.compare(lowerBound, other.lowerBound);
        return lowerCompareResult == 0 ? Long.compare(upperBound, other.upperBound) : lowerCompareResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Range that = (Range) o;

        return Objects.equal(this.lowerBound, that.lowerBound) &&
                Objects.equal(this.upperBound, that.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(lowerBound, upperBound);
    }

    @Override
    public String toString() {
        return lowerBound + "-" + upperBound;
    }

    /**
     * {@link Iterator} to generate values from lower to upper using a step.
     */
    public class StepIterator implements Iterator<Long> {
        private int step;
        private long current;

        public StepIterator(int step) {
            this.step = step;
            current = NumberUtils.roundDown(lowerBound, step);
        }

        @Override
        public boolean hasNext() {
            return current <= upperBound;
        }

        @Override
        public Long next() {
            long returnValue = current;
            current += step;
            return returnValue;
        }
    }

    public class StepIterable implements Iterable<Long> {
        private int step;

        public StepIterable(int step) {
            this.step = step;
        }

        @Override
        public Iterator<Long> iterator() {
            return new StepIterator(step);
        }
    }
}
