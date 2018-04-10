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

public class DefaultMetricsCheckerTest {

    final DefaultMetricsChecker.DeduplicatorMetricsChecker
            deduplicationMetricsChecker = new DefaultMetricsChecker.DeduplicatorMetricsChecker(10, 10);

    @Test
    public void duplicateMetricsCountingCheckIsDuplicate() {
        final Counting counter = mock(Counting.class);
        when(counter.getCount()).thenReturn(1L);
        assertFalse (deduplicationMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));
        assertTrue (deduplicationMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));

        when(counter.getCount()).thenReturn(2L);
        assertFalse (deduplicationMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));

        when(counter.getCount()).thenReturn(2L);

        final Map<String, String> tags = Maps.newHashMap();
        assertTrue (deduplicationMetricsChecker.isDuplicate("a", counter, tags));
        tags.put("b", "c");
        assertFalse (deduplicationMetricsChecker.isDuplicate("a", counter, tags));
    }
    
    @Test
    public void duplicateMetricsGuageCheckIsDuplicate() {
        final Gauge counter = mock(Gauge.class);
        when(counter.getValue()).thenReturn(Long.valueOf(1L));
        assertFalse (deduplicationMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));
        assertTrue (deduplicationMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));

        when(counter.getValue()).thenReturn(2L);
        assertFalse (deduplicationMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));

        when(counter.getValue()).thenReturn(2L);

        final Map<String, String> tags = Maps.newHashMap();
        assertTrue (deduplicationMetricsChecker.isDuplicate("a", counter, tags));
        tags.put("b", "c");
        assertFalse (deduplicationMetricsChecker.isDuplicate("a", counter, tags));
    }

    @Test
    public void deduplicatorMetricsExceptionCheckIsDuplicate() {
        final Gauge counter = mock(Gauge.class);
        when(counter.getValue()).thenThrow(new UnsupportedOperationException("Unsupported Value:" + "null"));
        assertFalse (deduplicationMetricsChecker.isDuplicate("a", counter, EMPTY_MAP));
    }
}
