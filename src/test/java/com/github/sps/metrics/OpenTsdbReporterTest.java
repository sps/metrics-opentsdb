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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author Sean Scanlon <sean.scanlon@gmail.com>
 */
@RunWith(MockitoJUnitRunner.class)
public class OpenTsdbReporterTest {

    private OpenTsdbReporter reporter;

    @Mock
    private OpenTsdb opentsdb;

    @Mock
    private MetricRegistry registry;

    @Mock
    private Gauge gauge;

    @Mock
    private Counter counter;

    @Mock
    private Clock clock;

    @Mock
    private Timer timer;

    @Mock
    Timer.Context context;

    private final long timestamp = 1000198;

    private ArgumentCaptor<Set> captor;

    @Before
    public void setUp() throws Exception {
        captor = ArgumentCaptor.forClass(Set.class);
        when(timer.time()).thenReturn(context);
        when(registry.timer(anyString())).thenReturn(timer);
        reporter = OpenTsdbReporter.forRegistry(registry)
                .withClock(clock)
                .prefixedWith("prefix")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .withTags(Collections.singletonMap("foo", "bar"))
                .withBatchSize(100)
                .build(opentsdb);

        when(clock.getTime()).thenReturn(timestamp * 1000);

    }

    @Test
    public void testReportGauges() {
        when(gauge.getValue()).thenReturn(1L);
        reporter.report(this.map(MetricName.build("gauge"), gauge), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(), this.<Timer>map());
        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(1, metrics.size());
        OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals("prefix.gauge.value", metric.getMetric());
        assertEquals(1L, metric.getValue());
        assertEquals((Long) timestamp, metric.getTimestamp());
    }

    @Test
    public void testReportCounters() {

        when(counter.getCount()).thenReturn(2L);
        reporter.report(this.<Gauge>map(), this.map(MetricName.build("counter"), counter), this.<Histogram>map(), this.<Meter>map(), this.<Timer>map());
        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(1, metrics.size());
        OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals("prefix.counter.count", metric.getMetric());
        assertEquals((Long) timestamp, metric.getTimestamp());
        assertEquals(2L, metric.getValue());
    }

    @Test
    public void testReportHistogram() {

        final Histogram histogram = mock(Histogram.class);
        when(histogram.getCount()).thenReturn(1L);

        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.getMax()).thenReturn(2L);
        when(snapshot.getMean()).thenReturn(3.0);
        when(snapshot.getMin()).thenReturn(4L);
        when(snapshot.getStdDev()).thenReturn(5.0);
        when(snapshot.getMedian()).thenReturn(6.0);
        when(snapshot.get75thPercentile()).thenReturn(7.0);
        when(snapshot.get95thPercentile()).thenReturn(8.0);
        when(snapshot.get98thPercentile()).thenReturn(9.0);
        when(snapshot.get99thPercentile()).thenReturn(10.0);
        when(snapshot.get999thPercentile()).thenReturn(11.0);

        when(histogram.getSnapshot()).thenReturn(snapshot);

        reporter.report(this.<Gauge>map(), this.<Counter>map(), this.map(MetricName.build("histogram"), histogram), this.<Meter>map(), this.<Timer>map());

        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(11, metrics.size());

        final OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals((Long) timestamp, metric.getTimestamp());

        final Map<String, Object> histMap = new HashMap<>();
        for (OpenTsdbMetric m : metrics) {
            histMap.put(m.getMetric(), m.getValue());
        }

        assertEquals(histMap.get("prefix.histogram.count"), 1L);
        assertEquals(histMap.get("prefix.histogram.max"), 2L);
        assertEquals(histMap.get("prefix.histogram.mean"), 3.0);
        assertEquals(histMap.get("prefix.histogram.min"), 4L);

        assertEquals((Double) histMap.get("prefix.histogram.stddev"), 5.0, 0.0001);
        assertEquals((Double) histMap.get("prefix.histogram.median"), 6.0, 0.0001);
        assertEquals((Double) histMap.get("prefix.histogram.p75"), 7.0, 0.0001);
        assertEquals((Double) histMap.get("prefix.histogram.p95"), 8.0, 0.0001);
        assertEquals((Double) histMap.get("prefix.histogram.p98"), 9.0, 0.0001);
        assertEquals((Double) histMap.get("prefix.histogram.p99"), 10.0, 0.0001);
        assertEquals((Double) histMap.get("prefix.histogram.p999"), 11.0, 0.0001);

    }

    @Test
    public void testReportTimers() {

        final Timer timer = mock(Timer.class);
        when(timer.getCount()).thenReturn(1L);
        when(timer.getMeanRate()).thenReturn(1.0);
        when(timer.getOneMinuteRate()).thenReturn(2.0);
        when(timer.getFiveMinuteRate()).thenReturn(3.0);
        when(timer.getFifteenMinuteRate()).thenReturn(4.0);

        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.getMax()).thenReturn(2L);
        when(snapshot.getMin()).thenReturn(4L);
        when(snapshot.getMean()).thenReturn(3.0);
        when(snapshot.getStdDev()).thenReturn(5.0);
        when(snapshot.getMedian()).thenReturn(6.0);
        when(snapshot.get75thPercentile()).thenReturn(7.0);
        when(snapshot.get95thPercentile()).thenReturn(8.0);
        when(snapshot.get98thPercentile()).thenReturn(9.0);
        when(snapshot.get99thPercentile()).thenReturn(10.0);
        when(snapshot.get999thPercentile()).thenReturn(11.0);

        when(timer.getSnapshot()).thenReturn(snapshot);

        reporter.report(this.<Gauge>map(), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(), this.map(MetricName.build("timer"), timer));

        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(15, metrics.size());

        final OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals((Long) timestamp, metric.getTimestamp());

        final Map<String, Object> timerMap = new HashMap<String, Object>();
        for (OpenTsdbMetric m : metrics) {
            timerMap.put(m.getMetric(), m.getValue());
        }

        assertEquals(timerMap.get("prefix.timer.count"), 1L);

        // duration should be in milliseconds, so we convert them to 1E-6 before output
        assertEquals((Double) timerMap.get("prefix.timer.max"), 2E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.mean"), 3.0E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.min"), 4E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.stddev"), 5.0E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.p75"), 7.0E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.p95"), 8.0E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.p98"), 9.0E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.p99"), 10.0E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.p999"), 11.0E-6, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.median"), 6.0E-6, 0.0001);

        //convert rate to seconds,
        assertEquals((Double) timerMap.get("prefix.timer.mean_rate"), 1.0, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.m1"), 2.0, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.m5"), 3.0, 0.0001);
        assertEquals((Double) timerMap.get("prefix.timer.m15"), 4.0, 0.0001);
    }


    @Test
    public void testReportMeter() {

        final Meter meter = mock(Meter.class);
        when(meter.getCount()).thenReturn(1L);
        when(meter.getMeanRate()).thenReturn(1.0);
        when(meter.getOneMinuteRate()).thenReturn(2.0);
        when(meter.getFiveMinuteRate()).thenReturn(3.0);
        when(meter.getFifteenMinuteRate()).thenReturn(4.0);

        reporter.report(this.<Gauge>map(), this.<Counter>map(), this.<Histogram>map(), this.map(MetricName.build("meter"), meter), this.<Timer>map());

        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(5, metrics.size());

        final OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals((Long) timestamp, metric.getTimestamp());

        final Map<String, Object> meterMap = new HashMap<String, Object>();
        for (OpenTsdbMetric m : metrics) {
            meterMap.put(m.getMetric(), m.getValue());
        }

        assertEquals(meterMap.get("prefix.meter.count"), 1L);

        //convert rate to seconds,
        assertEquals((Double) meterMap.get("prefix.meter.mean_rate"), 1.0, 0.0001);
        assertEquals((Double) meterMap.get("prefix.meter.m1"), 2.0, 0.0001);
        assertEquals((Double) meterMap.get("prefix.meter.m5"), 3.0, 0.0001);
        assertEquals((Double) meterMap.get("prefix.meter.m15"), 4.0, 0.0001);
    }

    @Test
    public void testTaggedMetrics() {
    	final Map<String, String> tags = new HashMap<String, String>();
    	tags.put("a", "b");

    	Gauge<Integer> gauge = new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 1;
			}
		};

    	final Counter counter = new Counter();
    	final Histogram histogram = mock(Histogram.class);
        when(histogram.getCount()).thenReturn(1L);
        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.getMax()).thenReturn(2L);
        when(snapshot.getMean()).thenReturn(3.0);
        when(snapshot.getMin()).thenReturn(4L);
        when(snapshot.getStdDev()).thenReturn(5.0);
        when(snapshot.getMedian()).thenReturn(6.0);
        when(snapshot.get75thPercentile()).thenReturn(7.0);
        when(snapshot.get95thPercentile()).thenReturn(8.0);
        when(snapshot.get98thPercentile()).thenReturn(9.0);
        when(snapshot.get99thPercentile()).thenReturn(10.0);
        when(snapshot.get999thPercentile()).thenReturn(11.0);

        when(histogram.getSnapshot()).thenReturn(snapshot);
    	final Meter meter = new Meter();
    	final Timer timer = new Timer();

    	SortedMap<MetricName, Gauge> gauges = new TreeMap<>();
    	gauges.put(new MetricName("gauge", tags), gauge);
    	reporter.report(
    	        gauges,
                this.map(new MetricName("counter", tags), counter),
                this.map(new MetricName("histogram", tags), histogram),
                this.map(new MetricName("meter", tags), meter),
                this.map(new MetricName("timer", tags), timer));

    	verify(opentsdb).send(captor.capture());
    	final Set<OpenTsdbMetric> metrics = captor.getValue();

        final Map<String, String> expectedTags = new HashMap<>(tags);
    	expectedTags.put("foo", "bar");

        for(OpenTsdbMetric metric : metrics) {
        	assertEquals(expectedTags, metric.getTags());
        }
    }

    /**
     * This tests that empty metrics sets will not be sent to the OpenTSDB server which
     * (at the time of authoring this test) fails to parse the JSON even though it is perfectly valid.
     * it is a particular problem with dropwizard's jvm.threads.deadlocks metric which contains an empty Set<String>
     * under normal operating conditions (i.e.: no deadlocks)
     */
    @Test
    public void testEmptyGaugeSet() {
        final Gauge gauge = mock(Gauge.class);
        when(gauge.getValue()).thenReturn(new HashSet<String>());
        reporter.report(this.map(MetricName.build("gauge"), gauge), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(), this.<Timer>map());

        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(0, metrics.size());
    }

    @Test
    public void testPerMetricTags() {

        when(counter.getCount()).thenReturn(2L);
        MetricName name = new MetricName("counter", Collections.singletonMap("foo2", "bar2"));
        reporter.report(this.<Gauge>map(), this.map(name, counter), this.<Histogram>map(), this.<Meter>map(), this.<Timer>map());
        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(1, metrics.size());
        OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals("prefix.counter.count", metric.getMetric());
        assertEquals((Long) timestamp, metric.getTimestamp());
        assertEquals(2L, metric.getValue());

        Map<String,String> tags = metric.getTags();
        assertEquals(2, tags.size());
        assertTrue(tags.containsKey("foo")); // applied to all metrics
        assertEquals("bar", tags.get("foo"));
        assertTrue(tags.containsKey("foo2")); // applied to just this counter
        assertEquals("bar2", tags.get("foo2"));
    }


    @Test
    public void testWithNoPrefix() {
        reporter = OpenTsdbReporter.forRegistry(registry)
                .withClock(clock)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .withTags(Collections.singletonMap("foo", "bar"))
                .withBatchSize(100)
                .build(opentsdb);

        when(counter.getCount()).thenReturn(2L);
        MetricName name = new MetricName("counter", Collections.singletonMap("foo2", "bar2"));
        reporter.report(this.<Gauge>map(), this.map(name, counter), this.<Histogram>map(), this.<Meter>map(), this.<Timer>map());
        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(1, metrics.size());
        OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals("counter.count", metric.getMetric());
        assertEquals((Long) timestamp, metric.getTimestamp());
        assertEquals(2L, metric.getValue());
    }


    @Test
    public void testDecorateDisabledCounter() {
        reporter = OpenTsdbReporter.forRegistry(registry)
                .withClock(clock)
                .prefixedWith("prefix")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .withTags(Collections.singletonMap("foo", "bar"))
                .withBatchSize(100)
                .withCounterGaugeDecorations(false)
                .build(opentsdb);

        when(counter.getCount()).thenReturn(2L);

        MetricName name = new MetricName("counter", Collections.singletonMap("foo2", "bar2"));
        reporter.report(this.<Gauge>map(), this.map(name, counter), this.<Histogram>map(), this.<Meter>map(), this.<Timer>map());
        verify(opentsdb).send(captor.capture());

        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(1, metrics.size());
        OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals("prefix.counter", metric.getMetric());
        assertEquals((Long) timestamp, metric.getTimestamp());
        assertEquals(2L, metric.getValue());
    }

    @Test
    public void testDecorateDisabledGauge() {
        reporter = OpenTsdbReporter.forRegistry(registry)
                .withClock(clock)
                .prefixedWith("prefix")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .withTags(Collections.singletonMap("foo", "bar"))
                .withBatchSize(100)
                .withCounterGaugeDecorations(false)
                .build(opentsdb);

        when(gauge.getValue()).thenReturn(1L);
        reporter.report(this.map(MetricName.build("gauge"), gauge), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(), this.<Timer>map());
        verify(opentsdb).send(captor.capture());
        final Set<OpenTsdbMetric> metrics = captor.getValue();
        assertEquals(1, metrics.size());
        OpenTsdbMetric metric = metrics.iterator().next();
        assertEquals("prefix.gauge", metric.getMetric());
        assertEquals(1L, metric.getValue());
        assertEquals((Long) timestamp, metric.getTimestamp());
    }


    private <T> SortedMap<MetricName, T> map() {
        return new TreeMap<>();
    }

    private <T> SortedMap<MetricName, T> map(MetricName name, T metric) {
        final TreeMap<MetricName, T> map = new TreeMap<>();
        map.put(name, metric);
        return map;
    }
}
