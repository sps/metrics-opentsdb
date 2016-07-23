package com.github.sps.metrics;

import java.util.Map;

import com.codahale.metrics.Meter;

public class TaggedMeter extends Meter implements TaggedMetric {

	private Map<String, String> tags;
	
	public TaggedMeter(Map<String, String> tags) {
		this.tags = tags;
	}

	@Override
	public Map<String, String> getTags() {
		return tags;
	}
}
