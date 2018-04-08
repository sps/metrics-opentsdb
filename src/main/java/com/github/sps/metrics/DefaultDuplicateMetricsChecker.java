package com.github.sps.metrics;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;

import java.util.Map;

class DefaultDuplicateMetricsChecker implements IDuplicateMetricsChecker {

    @Override
    public boolean isDuplicate(String key, Counting metric, Map<String, String> tagsToUse) {
        return false;
    }

    @Override
    public boolean isDuplicate(String key, Gauge metric, Map<String, String> tagsToUse) {
        return false;
    }
}
