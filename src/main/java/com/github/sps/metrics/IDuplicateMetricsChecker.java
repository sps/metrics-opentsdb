package com.github.sps.metrics;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;

import java.util.Map;

interface IDuplicateMetricsChecker {
    public boolean isDuplicate(String key, final Counting metric, Map<String, String> tagsToUse);

    public boolean isDuplicate(String key, final Gauge metric, Map<String, String> tagsToUse);

}

