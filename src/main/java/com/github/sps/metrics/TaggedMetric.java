package com.github.sps.metrics;

import java.util.Map;

import com.codahale.metrics.Metric;

public interface TaggedMetric extends Metric {
	public Map<String, String> getTags();
}
