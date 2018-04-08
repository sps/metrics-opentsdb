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

import static com.codahale.metrics.MetricAttribute.*;
import static java.util.Collections.EMPTY_SET;

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
    private final Set<MetricAttribute> disabledMetricAttributes;
    private final Timer timeToBuildReport;
    private final Timer timeToSendReport;

    private boolean decorateCounters = true;
    private boolean decorateGauges = true;

    private final DefaultMetricsChecker duplicate;

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
        private Set<MetricAttribute> disabledMetricAttributes;
        private int batchSize;
        private boolean decorateCounters;
        private boolean decorateGauges;
        private long deDupMaxMetrics;
        private int deDupTTL;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = "";
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.disabledMetricAttributes = Collections.emptySet();
            this.batchSize = OpenTsdb.DEFAULT_BATCH_SIZE_LIMIT;
            this.decorateCounters = true;
            this.decorateGauges = true;
            this.deDupMaxMetrics = 0;
            this.deDupTTL = 30;
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
         * Don't report the metric attributes contained within the given set.
         *
         * @param disabledMetricAttributes a set of {@link MetricAttribute}
         * @return {@code this}
         */
        public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
            this.disabledMetricAttributes = disabledMetricAttributes;
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
         * 
         * @param maxItems check Max Items, if zero to disable
         * @param ttlMinutes items TTL
         * @return {@code this}
         */
        public Builder withDeduplicator(long maxItems, int ttlMinutes) {
            this.deDupMaxMetrics = maxItems;
            this.deDupTTL = ttlMinutes;
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
                    filter,
                    tags,
                    disabledMetricAttributes,
                    decorateCounters, decorateGauges, deDupMaxMetrics, deDupTTL);
        }
    }

    private static class MetricsCollector {
        private final String prefix;
        private final Map<String, String> tags;
        private final long timestamp;
        private final Set<OpenTsdbMetric> metrics = new HashSet<OpenTsdbMetric>();
        private final Set<MetricAttribute> disabledMetricAttributes;

        private MetricsCollector(String prefix, Map<String, String> tags, Set<MetricAttribute> disabledMetricAttributes, long timestamp) {
            this.prefix = prefix;
            this.tags = tags;
            this.timestamp = timestamp;
            this.disabledMetricAttributes = disabledMetricAttributes;
        }

        public static MetricsCollector createNew(String prefix, Map<String, String> tags, Set<MetricAttribute> disabledMetricAttributes, long timestamp) {
            return new MetricsCollector(prefix, tags, disabledMetricAttributes, timestamp);
        }

        public MetricsCollector addMetric(String metricName, Object value) {
            this.metrics.add(OpenTsdbMetric.named(MetricRegistry.name(prefix, metricName))
                    .withTimestamp(timestamp)
                    .withValue(value)
                    .withTags(tags).build());
            return this;
        }

        public MetricsCollector addMetricIfEnabled(MetricAttribute attribute, Object value) {
            if (disabledMetricAttributes.contains(attribute)) {
                return this;
            } else {
                return addMetric(attribute.getCode(), value);
            }
        }

        public MetricsCollector addMetricIfEnabled(MetricAttribute attribute, String metricName, Object value) {
            if (disabledMetricAttributes.contains(attribute)) {
                return this;
            } else {
                return addMetric(metricName, value);
            }
        }

        public Set<OpenTsdbMetric> build() {
            return metrics;
        }
    }

    private OpenTsdbReporter(MetricRegistry registry, OpenTsdb opentsdb, Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, Map<String, String> tags, Set<MetricAttribute> disabledMetricAttributes, boolean decorateCounters, boolean decorateGauges, long deDupMetrics, int deDupTTL) {
        super(registry, "opentsdb-reporter", filter, rateUnit, durationUnit, null, true, disabledMetricAttributes);
        this.opentsdb = opentsdb;
        this.clock = clock;
        this.prefix = prefix;
        this.tags = tags;
        this.disabledMetricAttributes = disabledMetricAttributes;
        this.timeToSendReport = registry.timer("open-tsdb-reporter-time-to-send-report");
        this.timeToBuildReport = registry.timer("open-tsdb-reporter-time-to-build-report");
        this.decorateCounters = decorateCounters;
        this.decorateGauges = decorateGauges;
        if (deDupMetrics > 0) {
            this.duplicate = new DeduplicatorMetricsChecker(deDupMetrics, deDupTTL);
        } else {
            this.duplicate = new DefaultMetricsChecker();
        }
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    	final Timer.Context context = timeToBuildReport.time();
        final long timestamp = clock.getTime() / 1000;

        final Set<OpenTsdbMetric> metrics = new HashSet<OpenTsdbMetric>();
        
        for (Map.Entry<String, Gauge> g : gauges.entrySet()) {
            if(g.getValue().getValue() instanceof Collection && ((Collection)g.getValue().getValue()).isEmpty()) {
                continue;
            }
            
            Map<String, String> tagsToUse = new HashMap<String, String>(tags);
            String key = g.getKey();
        	if(g.getValue() instanceof TaggedMetric ) {
        		key = TaggedMetricRegistry.getBaseName(key);
        		Map<String, String> objectTags = ((TaggedMetric) g.getValue()).getTags();
        		if(objectTags != null) {
        			tagsToUse.putAll(objectTags);
        		}
        	}
            if (!this.duplicate.isDuplicate(key, g.getValue(), tagsToUse)) {
                metrics.add(buildGauge(key, g.getValue(), timestamp, tagsToUse));
            }
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        	Map<String, String> tagsToUse = new HashMap<String, String>(tags);
        	String key = entry.getKey();
        	if(entry.getValue() instanceof TaggedCounter) {
        		key = TaggedMetricRegistry.getBaseName(key);
        		Map<String, String> objectTags = ((TaggedCounter) entry.getValue()).getTags();
        		if(objectTags != null) {
        			tagsToUse.putAll(objectTags);
        		}
        	}
            metrics.addAll(buildCounter(key, entry.getValue(), timestamp, tagsToUse));
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
        	Map<String, String> tagsToUse = new HashMap<String, String>(tags);
        	String key = entry.getKey();
        	if(entry.getValue() instanceof TaggedHistogram) {
        		key = TaggedMetricRegistry.getBaseName(key);
        		Map<String, String> objectTags = ((TaggedHistogram) entry.getValue()).getTags();
        		if(objectTags != null) {
        			tagsToUse.putAll(objectTags);
        		}
        	}
            metrics.addAll(buildHistograms(key, entry.getValue(), timestamp, tagsToUse));
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        	Map<String, String> tagsToUse = new HashMap<String, String>(tags);
        	String key = entry.getKey();
        	if(entry.getValue() instanceof TaggedMeter) {
        		key = TaggedMetricRegistry.getBaseName(key);
        		Map<String, String> objectTags = ((TaggedMeter) entry.getValue()).getTags();
        		if(objectTags != null) {
        			tagsToUse.putAll(objectTags);
        		}
        	}
            metrics.addAll(buildMeters(key, entry.getValue(), timestamp, tagsToUse));
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
        	Map<String, String> tagsToUse = new HashMap<String, String>(tags);
        	String key = entry.getKey();
        	if(entry.getValue() instanceof TaggedTimer) {
        		key = TaggedMetricRegistry.getBaseName(key);
        		Map<String, String> objectTags = ((TaggedTimer) entry.getValue()).getTags();
        		if(objectTags != null) {
        			tagsToUse.putAll(objectTags);
        		}
        	}
            metrics.addAll(buildTimers(key, entry.getValue(), timestamp, tagsToUse));
        }
        context.stop();
        
        final Timer.Context context2 = timeToSendReport.time();
        opentsdb.send(metrics);
        context2.stop();
        
    }

    private Set<OpenTsdbMetric> buildTimers(String name, Timer timer, long timestamp, Map<String, String> tags) {
        if (this.duplicate.isDuplicate(name, timer, tags)) {
            return EMPTY_SET;
        }
        final MetricsCollector collector = MetricsCollector.createNew(prefix(name), tags, disabledMetricAttributes, timestamp);
        final Snapshot snapshot = timer.getSnapshot();

        return collector.addMetricIfEnabled(COUNT, timer.getCount())
                //convert rate
                .addMetricIfEnabled(M15_RATE, convertRate(timer.getFifteenMinuteRate()))
                .addMetricIfEnabled(M5_RATE, convertRate(timer.getFiveMinuteRate()))
                .addMetricIfEnabled(M1_RATE, convertRate(timer.getOneMinuteRate()))
                .addMetricIfEnabled(MEAN_RATE, convertRate(timer.getMeanRate()))
                // convert duration
                .addMetricIfEnabled(MAX, convertDuration(snapshot.getMax()))
                .addMetricIfEnabled(MIN, convertDuration(snapshot.getMin()))
                .addMetricIfEnabled(MEAN, convertDuration(snapshot.getMean()))
                .addMetricIfEnabled(STDDEV, convertDuration(snapshot.getStdDev()))
                .addMetricIfEnabled(P50, convertDuration(snapshot.getMedian()))
                .addMetricIfEnabled(P75, convertDuration(snapshot.get75thPercentile()))
                .addMetricIfEnabled(P95, convertDuration(snapshot.get95thPercentile()))
                .addMetricIfEnabled(P98, convertDuration(snapshot.get98thPercentile()))
                .addMetricIfEnabled(P99, convertDuration(snapshot.get99thPercentile()))
                .addMetricIfEnabled(P999, convertDuration(snapshot.get999thPercentile()))
                .build();
    }

    private Set<OpenTsdbMetric> buildHistograms(String name, Histogram histogram, long timestamp, Map<String, String> tags) {
        if (this.duplicate.isDuplicate(name, histogram, tags)) {
            return EMPTY_SET;
        }
        final MetricsCollector collector = MetricsCollector.createNew(prefix(name), tags, disabledMetricAttributes, timestamp);
        final Snapshot snapshot = histogram.getSnapshot();

        return collector.addMetricIfEnabled(COUNT, histogram.getCount())
                .addMetricIfEnabled(MAX, snapshot.getMax())
                .addMetricIfEnabled(MIN, snapshot.getMin())
                .addMetricIfEnabled(MEAN, snapshot.getMean())
                .addMetricIfEnabled(STDDEV, snapshot.getStdDev())
                .addMetricIfEnabled(P50, snapshot.getMedian())
                .addMetricIfEnabled(P75, snapshot.get75thPercentile())
                .addMetricIfEnabled(P95, snapshot.get95thPercentile())
                .addMetricIfEnabled(P98, snapshot.get98thPercentile())
                .addMetricIfEnabled(P99, snapshot.get99thPercentile())
                .addMetricIfEnabled(P999, snapshot.get999thPercentile())
                .build();
    }

    private Set<OpenTsdbMetric> buildMeters(String name, Meter meter, long timestamp, Map<String, String> tags) {
        if (this.duplicate.isDuplicate(name, meter, tags)) {
            return EMPTY_SET;
        }
        final MetricsCollector collector = MetricsCollector.createNew(prefix(name), tags, disabledMetricAttributes, timestamp);

        return collector.addMetricIfEnabled(COUNT, meter.getCount())
                // convert rate
                .addMetricIfEnabled(MEAN_RATE, convertRate(meter.getMeanRate()))
                .addMetricIfEnabled(M1_RATE, convertRate(meter.getOneMinuteRate()))
                .addMetricIfEnabled(M5_RATE, convertRate(meter.getFiveMinuteRate()))
                .addMetricIfEnabled(M15_RATE, convertRate(meter.getFifteenMinuteRate()))
                .build();
    }

    private Set<OpenTsdbMetric> buildCounter(String name, Counter counter, long timestamp, Map<String, String> tags) {
        if (this.duplicate.isDuplicate(name, counter, tags)) {
            return EMPTY_SET;
        }
        return MetricsCollector.createNew(prefix(name), tags, disabledMetricAttributes, timestamp)
                .addMetricIfEnabled(COUNT, decorateCounters ? "count" : "", counter.getCount())
                .build();
    }

    private OpenTsdbMetric buildGauge(String name, Gauge gauge, long timestamp, Map<String, String> tags) {
        return OpenTsdbMetric.named(decorateGauges ? prefix(name, "value") : prefix(name))
                .withValue(gauge.getValue())
                .withTimestamp(timestamp)
                .withTags(tags)
                .build();
    }

    private String prefix(String... components) {
        if (prefix.length() == 0)
            return MetricRegistry.name(prefix, components);
        else
            return OpenTsdbMetric.fixEncodedTagsInNameAfterPrefix(MetricRegistry.name(prefix, components));
    }

}
