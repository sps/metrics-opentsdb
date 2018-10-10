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

import io.dropwizard.metrics5.*;
import io.dropwizard.metrics5.Timer;
import com.github.sps.metrics.opentsdb.OpenTsdb;
import com.github.sps.metrics.opentsdb.OpenTsdbMetric;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a OpenTSDB server.
 *
 * @author Sean Scanlon <sean.scanlon@gmail.com>
 * @author Randy Buck <buck@adobe.com>
 * @author alugowski (modified for Turn)
 */
public class OpenTsdbReporter extends ScheduledReporter {

    private final OpenTsdb opentsdb;
    private final Clock clock;
    private final String prefix;
    private final Map<String, String> tags;
    private final Timer timeToBuildReport;
    private final Timer timeToSendReport;

    private boolean decorateCounters;
    private boolean decorateGauges;

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
        private int batchSize;
        private boolean decorateCounters;
        private boolean decorateGauges;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = "";
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.batchSize = OpenTsdb.DEFAULT_BATCH_SIZE_LIMIT;
            this.decorateCounters = true;
            this.decorateGauges = true;
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
         * @return {@code this}
         */
        public Builder withTags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * Enable decorating Counter metric names with {@code .count} and Gauge metric names with
         * {@code .value}.
         *
         * @param withCounterGaugeDecorations
         * @return {@code this}
         */
        public Builder withCounterGaugeDecorations(boolean withCounterGaugeDecorations) {
            this.decorateCounters = withCounterGaugeDecorations;
            this.decorateGauges = withCounterGaugeDecorations;
            return this;
        }

        /**
         * specify number of metrics send in each request
         *
         * @param batchSize
         * @return {@code this}
         */
        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
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
            opentsdb.setBatchSizeLimit(batchSize);
            return new OpenTsdbReporter(registry,
                    opentsdb,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter, tags, decorateCounters, decorateGauges);
        }
    }

    private static class MetricsCollector {
        private final MetricName prefix;
        private final long timestamp;
        private final Set<OpenTsdbMetric> metrics = new HashSet<>();

        private MetricsCollector(MetricName prefix, long timestamp) {
            this.prefix = prefix;
            this.timestamp = timestamp;
        }

        public static MetricsCollector createNew(MetricName name, long timestamp) {
            return new MetricsCollector(name, timestamp);
        }

        public MetricsCollector addMetric(String metricName, Object value) {
            this.metrics.add(OpenTsdbMetric.tagged(prefix.resolve(metricName))
                    .withTimestamp(timestamp)
                    .withValue(value)
                    .build());
            return this;
        }

        public Set<OpenTsdbMetric> build() {
            return metrics;
        }
    }

    private OpenTsdbReporter(MetricRegistry registry, OpenTsdb opentsdb, Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, Map<String, String> tags, boolean decorateCounters, boolean decorateGauges) {
        super(registry, "opentsdb-reporter", filter, rateUnit, durationUnit);
        this.opentsdb = opentsdb;
        this.clock = clock;
        this.prefix = prefix;
        this.tags = tags;
        this.timeToSendReport = registry.timer("open-tsdb-reporter-time-to-send-report");
        this.timeToBuildReport = registry.timer("open-tsdb-reporter-time-to-build-report");
        this.decorateCounters = decorateCounters;
        this.decorateGauges = decorateGauges;
    }

    @Override
    public void report(
            SortedMap<MetricName, Gauge> gauges,
            SortedMap<MetricName, Counter> counters,
            SortedMap<MetricName, Histogram> histograms,
            SortedMap<MetricName, Meter> meters,
            SortedMap<MetricName, Timer> timers) {

    	final Timer.Context context = timeToBuildReport.time();
        final long timestamp = clock.getTime() / 1000;

        final Set<OpenTsdbMetric> metrics = new HashSet<>();
        
        for (Map.Entry<MetricName, Gauge> g : gauges.entrySet()) {
            if(g.getValue().getValue() instanceof Collection && ((Collection)g.getValue().getValue()).isEmpty()) {
                continue;
            }

            metrics.add(buildGauge(g.getKey(), g.getValue(), timestamp));
        }

        for (Map.Entry<MetricName, Counter> entry : counters.entrySet()) {
            metrics.add(buildCounter(entry.getKey(), entry.getValue(), timestamp));
        }

        for (Map.Entry<MetricName, Histogram> entry : histograms.entrySet()) {
            metrics.addAll(buildHistograms(entry.getKey(), entry.getValue(), timestamp));
        }

        for (Map.Entry<MetricName, Meter> entry : meters.entrySet()) {
            metrics.addAll(buildMeters(entry.getKey(), entry.getValue(), timestamp));
        }

        for (Map.Entry<MetricName, Timer> entry : timers.entrySet()) {
            metrics.addAll(buildTimers(entry.getKey(), entry.getValue(), timestamp));
        }
        context.stop();
        
        final Timer.Context context2 = timeToSendReport.time();
        opentsdb.send(metrics);
        context2.stop();
        
    }

    private Set<OpenTsdbMetric> buildTimers(MetricName name, Timer timer, long timestamp) {
        final MetricsCollector collector = MetricsCollector.createNew(prefix(name).tagged(tags), timestamp);
        final Snapshot snapshot = timer.getSnapshot();

        return collector.addMetric("count", timer.getCount())
                //convert rate
                .addMetric("m15", convertRate(timer.getFifteenMinuteRate()))
                .addMetric("m5", convertRate(timer.getFiveMinuteRate()))
                .addMetric("m1", convertRate(timer.getOneMinuteRate()))
                .addMetric("mean_rate", convertRate(timer.getMeanRate()))
                // convert duration
                .addMetric("max", convertDuration(snapshot.getMax()))
                .addMetric("min", convertDuration(snapshot.getMin()))
                .addMetric("mean", convertDuration(snapshot.getMean()))
                .addMetric("stddev", convertDuration(snapshot.getStdDev()))
                .addMetric("median", convertDuration(snapshot.getMedian()))
                .addMetric("p75", convertDuration(snapshot.get75thPercentile()))
                .addMetric("p95", convertDuration(snapshot.get95thPercentile()))
                .addMetric("p98", convertDuration(snapshot.get98thPercentile()))
                .addMetric("p99", convertDuration(snapshot.get99thPercentile()))
                .addMetric("p999", convertDuration(snapshot.get999thPercentile()))
                .build();
    }

    private Set<OpenTsdbMetric> buildHistograms(MetricName name, Histogram histogram, long timestamp) {

        final MetricsCollector collector = MetricsCollector.createNew(prefix(name).tagged(tags), timestamp);
        final Snapshot snapshot = histogram.getSnapshot();

        return collector.addMetric("count", histogram.getCount())
                .addMetric("max", snapshot.getMax())
                .addMetric("min", snapshot.getMin())
                .addMetric("mean", snapshot.getMean())
                .addMetric("stddev", snapshot.getStdDev())
                .addMetric("median", snapshot.getMedian())
                .addMetric("p75", snapshot.get75thPercentile())
                .addMetric("p95", snapshot.get95thPercentile())
                .addMetric("p98", snapshot.get98thPercentile())
                .addMetric("p99", snapshot.get99thPercentile())
                .addMetric("p999", snapshot.get999thPercentile())
                .build();
    }

    private Set<OpenTsdbMetric> buildMeters(MetricName name, Meter meter, long timestamp) {

        final MetricsCollector collector = MetricsCollector.createNew(prefix(name).tagged(tags), timestamp);

        return collector.addMetric("count", meter.getCount())
                // convert rate
                .addMetric("mean_rate", convertRate(meter.getMeanRate()))
                .addMetric("m1", convertRate(meter.getOneMinuteRate()))
                .addMetric("m5", convertRate(meter.getFiveMinuteRate()))
                .addMetric("m15", convertRate(meter.getFifteenMinuteRate()))
                .build();
    }

    private OpenTsdbMetric buildCounter(MetricName name, Counter counter, long timestamp) {
        return OpenTsdbMetric.tagged(prefix(decorateCounters ? name.resolve("count") : name))
                .withTimestamp(timestamp)
                .withValue(counter.getCount())
                .withTags(tags)
                .build();
    }

    private OpenTsdbMetric buildGauge(MetricName name, Gauge gauge, long timestamp) {
        return OpenTsdbMetric.tagged(prefix(decorateGauges ? name.resolve("value") : name))
                .withValue(gauge.getValue())
                .withTimestamp(timestamp)
                .withTags(tags)
                .build();
    }

    private MetricName prefix(MetricName name) {
        if (prefix.length() == 0)
            return name;
        else
            return MetricRegistry.name(prefix).append(name);
    }
}
