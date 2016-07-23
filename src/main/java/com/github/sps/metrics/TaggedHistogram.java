package com.github.sps.metrics;

import java.util.Map;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;

public class TaggedHistogram extends Histogram implements TaggedMetric {

	private Map<String, String> tags;

	public TaggedHistogram(Reservoir reservoir, Map<String, String> tags) {
		super(reservoir);
		this.tags = tags;
	}

	@Override
	public Map<String, String> getTags() {
		return tags;
	}

}
