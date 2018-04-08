package com.github.sps.metrics;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static java.util.Collections.EMPTY_MAP;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeduplicatorMetricsCheckerTest {

    final DeduplicatorMetricsChecker DeduplicatorMetricsChecker = new DeduplicatorMetricsChecker(10, 10);

    @Test
    public void duplicateMetricsCountingCheckIsDuplicate() {
        final Counting counter = mock(Counting.class);
        when(counter.getCount()).thenReturn(1L);
        assertFalse (DeduplicatorMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));
        assertTrue (DeduplicatorMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));

        when(counter.getCount()).thenReturn(2L);
        assertFalse (DeduplicatorMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));

        when(counter.getCount()).thenReturn(2L);

        final Map<String, String> tags = Maps.newHashMap();
        assertTrue (DeduplicatorMetricsChecker.isDuplicate("a", counter, tags));
        tags.put("b", "c");
        assertFalse (DeduplicatorMetricsChecker.isDuplicate("a", counter, tags));
    }


    @Test
    public void duplicateMetricsGuageCheckIsDuplicate() {
        final Gauge counter = mock(Gauge.class);
        when(counter.getValue()).thenReturn(1L);
        assertFalse (DeduplicatorMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));
        assertTrue (DeduplicatorMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));

        when(counter.getValue()).thenReturn(2L);
        assertFalse (DeduplicatorMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));

        when(counter.getValue()).thenReturn(2L);

        final Map<String, String> tags = Maps.newHashMap();
        assertTrue (DeduplicatorMetricsChecker.isDuplicate("a", counter, tags));
        tags.put("b", "c");
        assertFalse (DeduplicatorMetricsChecker.isDuplicate("a", counter, tags));
    }

    @Test
    public void deduplicatorMetricsExceptionCheckIsDuplicate() {
        final Gauge counter = mock(Gauge.class);
        when(counter.getValue()).thenThrow(new UnsupportedOperationException("UnSupport Type:" + "null"));
        assertFalse (DeduplicatorMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));
    }
}
