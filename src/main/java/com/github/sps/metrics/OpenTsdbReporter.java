/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.sps.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.github.sps.metrics.opentsdb.OpenTsdb;
import com.github.sps.metrics.opentsdb.OpenTsdbMetric;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a OpenTSDB server.
 *
 * @author Sean Scanlon <sean.scanlon@gmail.com>
 */
public class OpenTsdbReporter extends ScheduledReporter {

    private final OpenTsdb opentsdb;
    private final Clock clock;
    private final String prefix;
    private final Map<String, String> tags;

    /**
     * Returns a new {@link Builder} for {@link OpenTsdbReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link OpenTsdbReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link OpenTsdbReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private Map<String, String> tags;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Append tags to all reported metrics
         *
         * @param tags
         * @return
         */
        public Builder withTags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * Builds a {@link OpenTsdbReporter} with the given properties, sending metrics using the
         * given {@link com.github.sps.metrics.opentsdb.OpenTsdb} client.
         *
         * @param opentsdb a {@link OpenTsdb} client
         * @return a {@link OpenTsdbReporter}
         */
        public OpenTsdbReporter build(OpenTsdb opentsdb) {
            return new OpenTsdbReporter(registry,
                    opentsdb,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter, tags);
        }
    }

    private static class MetricsCollector {
        private final String prefix;
        private final Map<String, String> tags;
        private final long timestamp;
        private Set<OpenTsdbMetric> metrics = new HashSet<OpenTsdbMetric>();

        private MetricsCollector(String prefix, Map<String, String> tags, long timestamp) {
            this.prefix = prefix;
            this.tags = tags;
            this.timestamp = timestamp;
        }

        public static MetricsCollector getInstance(String prefix, Map<String, String> tags, long timestamp) {
            return new MetricsCollector(prefix, tags, timestamp);
        }

        public MetricsCollector addMetric(String metricName, Object value) {
            this.metrics.add(OpenTsdbMetric.named(MetricRegistry.name(prefix, metricName))
                    .withTimestamp(timestamp)
                    .withValue(value)
                    .withTags(tags).build());
            return this;
        }

        public Set<OpenTsdbMetric> build() {
            return metrics;
        }
    }

    private OpenTsdbReporter(MetricRegistry registry, OpenTsdb opentsdb, Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, Map<String, String> tags) {
        super(registry, "opentsdb-reporter", filter, rateUnit, durationUnit);
        this.opentsdb = opentsdb;
        this.clock = clock;
        this.prefix = prefix;
        this.tags = tags;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

        final long timestamp = clock.getTime() / 1000;

        final Set<OpenTsdbMetric> metrics = new HashSet<OpenTsdbMetric>();

        for (Map.Entry<String, Gauge> g : gauges.entrySet()) {
            metrics.add(buildGauge(g.getKey(), g.getValue(), timestamp));
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            metrics.add(buildCounter(entry.getKey(), entry.getValue(), timestamp));
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            metrics.addAll(buildHistograms(entry.getKey(), entry.getValue(), timestamp));
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            metrics.addAll(buildTimers(entry.getKey(), entry.getValue(), timestamp));
        }

        opentsdb.send(metrics);
    }

    private Set<OpenTsdbMetric> buildTimers(String name, Timer timer, long timestamp) {

        final MetricsCollector collector = MetricsCollector.getInstance(prefix(name), tags, timestamp);
        final Snapshot snapshot = timer.getSnapshot();

        collector.addMetric("count", timer.getCount());
        collector.addMetric("m15", convertRate(timer.getFifteenMinuteRate()));
        collector.addMetric("m5", convertRate(timer.getFiveMinuteRate()));
        collector.addMetric("m1", convertRate(timer.getOneMinuteRate()));
        collector.addMetric("mean_rate", convertRate(timer.getMeanRate()));

        collector.addMetric("max", convertDuration(snapshot.getMax()));
        collector.addMetric("min", convertDuration(snapshot.getMin()));
        collector.addMetric("mean", convertDuration(snapshot.getMean()));
        collector.addMetric("stddev", convertDuration(snapshot.getStdDev()));
        collector.addMetric("median", convertDuration(snapshot.getMedian()));
        collector.addMetric("p75", convertDuration(snapshot.get75thPercentile()));
        collector.addMetric("p95", convertDuration(snapshot.get95thPercentile()));
        collector.addMetric("p98", convertDuration(snapshot.get98thPercentile()));
        collector.addMetric("p99", convertDuration(snapshot.get99thPercentile()));
        collector.addMetric("p999", convertDuration(snapshot.get999thPercentile()));

        return collector.build();
    }


    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

    private Set<OpenTsdbMetric> buildHistograms(String name, Histogram histogram, long timestamp) {

        final MetricsCollector collector = MetricsCollector.getInstance(prefix(name), tags, timestamp);
        final Snapshot snapshot = histogram.getSnapshot();

        collector.addMetric("count", histogram.getCount());
        collector.addMetric("max", snapshot.getMax());
        collector.addMetric("min", snapshot.getMin());
        collector.addMetric("mean", snapshot.getMean());
        collector.addMetric("stddev", snapshot.getStdDev());
        collector.addMetric("median", snapshot.getMedian());
        collector.addMetric("p75", snapshot.get75thPercentile());
        collector.addMetric("p95", snapshot.get95thPercentile());
        collector.addMetric("p98", snapshot.get98thPercentile());
        collector.addMetric("p99", snapshot.get99thPercentile());
        collector.addMetric("p999", snapshot.get999thPercentile());

        return collector.build();
    }

    private OpenTsdbMetric buildCounter(String name, Counter counter, long timestamp) {
        return OpenTsdbMetric.named(prefix(name, "count"))
                .withTimestamp(timestamp)
                .withValue(counter.getCount())
                .withTags(tags)
                .build();
    }


    private OpenTsdbMetric buildGauge(String name, Gauge gauge, long timestamp) {
        return OpenTsdbMetric.named(prefix(name))
                .withValue(gauge.getValue())
                .withTimestamp(timestamp)
                .withTags(tags)
                .build();
    }


}
