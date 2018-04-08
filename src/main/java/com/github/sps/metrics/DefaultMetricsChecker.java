package com.github.sps.metrics;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;

import java.util.Map;

class DefaultMetricsChecker {

    public boolean isDuplicate(String key, Counting metric, Map<String, String> tagsToUse) {
        return false;
    }

    public boolean isDuplicate(String key, Gauge metric, Map<String, String> tagsToUse) {
        return false;
    }
}
