package com.github.sps.metrics;

import java.util.Map;

import com.codahale.metrics.Counter;

public class TaggedCounter extends Counter implements TaggedMetric {

	private Map<String, String> tags;
	
	public TaggedCounter(Map<String, String> tags) {
		this.tags = tags;
	}

	@Override
	public Map<String, String> getTags() {
		return tags;
	}
}
