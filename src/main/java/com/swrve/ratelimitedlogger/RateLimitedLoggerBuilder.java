package com.swrve.ratelimitedlogger;

import java.time.Duration;

import org.slf4j.Logger;


import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Factory to create new RateLimitedLog instances in a fluent Builder style.  Start with
 * RateLimitedLog.withRateLimit(logger).
 */

public class RateLimitedLoggerBuilder {
    private final Logger logger;
    private final int maxRate;
    private final Duration periodLength;
    private Stopwatch stopwatch = new Stopwatch();
    private Optional<CounterMetric> stats = Optional.absent();

    public static class MissingRateAndPeriod {
        private final Logger logger;

        MissingRateAndPeriod(Logger logger) {
            this.logger = logger;
        }

        /**
         * Specify the maximum count of logs in every time period.  Required.
         */
        public MissingPeriod maxRate(int rate) {
            return new MissingPeriod(logger, rate);
        }
    }

    public static class MissingPeriod {
        private final Logger logger;
        private final int maxRate;

        private MissingPeriod(Logger logger, int rate) {
            Preconditions.checkNotNull(logger);
            this.logger = logger;
            this.maxRate = rate;
        }

        /**
         * Specify the time period.  Required.
         */
        public RateLimitedLoggerBuilder every(Duration duration) {
            Preconditions.checkNotNull(duration);
            return new RateLimitedLoggerBuilder(logger, maxRate, duration);
        }
    }

    private RateLimitedLoggerBuilder(Logger logger, int maxRate, Duration periodLength) {
        this.logger = logger;
        this.maxRate = maxRate;
        this.periodLength = periodLength;
    }

    /**
     * Specify that the rate-limited logger should compute time using @param stopwatch.
     */
    public RateLimitedLoggerBuilder withStopwatch(Stopwatch stopwatch) {
        this.stopwatch = Preconditions.checkNotNull(stopwatch);
        return this;
    }

    /**
     * Optional: should we record metrics about the call rate using @param stats.  Default is not to record metrics
     */
    public RateLimitedLoggerBuilder recordMetrics(CounterMetric stats) {
        this.stats = Optional.of(Preconditions.checkNotNull(stats));
        return this;
    }

    /**
     * @return a fully-built RateLimitedLog matching the requested configuration.
     */
    public RateLimitedLogger build() {
        Preconditions.checkArgument(maxRate > 0, "maxRate must be > 0");
        Preconditions.checkArgument(periodLength.toMillis() > 0L, "period must be non-zero");
        stopwatch.start();
        return new RateLimitedLogger(logger,
                new RateLimitedLogWithPattern.RateAndPeriod(maxRate, periodLength), stopwatch,
                stats, RateLimitedLogger.REGISTRY);
    }
}
