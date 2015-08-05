package com.github.sps.metrics;

import java.util.Map;

import com.codahale.metrics.Timer;

public class TaggedTimer extends Timer implements TaggedMetric {

	private Map<String, String> tags;
	
	public TaggedTimer(Map<String, String> tags) {
		this.tags = tags;
	}

	@Override
	public Map<String, String> getTags() {
		return tags;
	}
}
