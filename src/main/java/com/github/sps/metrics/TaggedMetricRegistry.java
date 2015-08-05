package com.github.sps.metrics;

import java.util.Map;
import java.util.regex.Pattern;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;

public class TaggedMetricRegistry extends MetricRegistry {

	public static final String delimiter = "~";
	private final Timer timeToFindMetric;
	private final Gauge<Integer> metricsCount;
	
	public TaggedMetricRegistry() {
		super();
		timeToFindMetric = this.timer("tagged-metric-registry-time-to-find-metric");
		metricsCount = new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return getMetrics().size();
			}
		};
		register("metrics-count", metricsCount);
	}
	
	public TaggedMetric getOrRegisterTaggedMetric(final String name, final TaggedMetric metric) {
		String taggedName = TaggedMetricRegistry.getTaggedName(name, metric.getTags());
		TaggedMetric registeredMetric;
		try {
			registeredMetric = register(taggedName, metric);
		} catch (IllegalArgumentException e) {
			registeredMetric = (TaggedMetric) getMetrics().get(taggedName);
		}
		return registeredMetric;
	}

	public TaggedCounter taggedCounter(final String name,
			final Map<String, String> tags) {
		final TaggedCounter counter = new TaggedCounter(tags);
		return (TaggedCounter) getOrRegisterTaggedMetric(name, counter);
	}

	public TaggedCounter getTaggedCounter(final String name,
			final Map<String, String> searchTags) {
		return (TaggedCounter) getTaggedMetric(name, searchTags);
	}

	public TaggedMeter taggedMeter(final String name,
			final Map<String, String> tags) {
		final TaggedMeter metric = new TaggedMeter(tags);
		return (TaggedMeter) getOrRegisterTaggedMetric(name, metric);
	}

	public TaggedMeter getTaggedMeter(final String name,
			final Map<String, String> searchTags) {
		return (TaggedMeter) getTaggedMetric(name, searchTags);
	}

	public TaggedHistogram taggedHistogram(final Reservoir reservoir,
			final String name, final Map<String, String> tags) {
		final TaggedMetric metric = new TaggedHistogram(reservoir, tags);
		return (TaggedHistogram) getOrRegisterTaggedMetric(name, metric);
	}

	public TaggedHistogram getTaggedHistogram(final String name,
			final Map<String, String> searchTags) {
		return (TaggedHistogram) getTaggedMetric(name, searchTags);
	}
	
	public TaggedTimer taggedTimer(final String name, final Map<String, String> tags) {
		final TaggedTimer metric = new TaggedTimer(tags);
		return (TaggedTimer) getOrRegisterTaggedMetric(name, metric);
	}

	public TaggedTimer getTaggedTimer(final String name,
			final Map<String, String> searchTags) {
		return (TaggedTimer) getTaggedMetric(name, searchTags);
	}

	public TaggedMetric getTaggedMetric(final String name,
			final Map<String, String> searchTags) {
		final Timer.Context context = timeToFindMetric.time();
		for (Map.Entry<String, Metric> entry : getMetrics().entrySet()) {
			if (!(entry.getValue() instanceof TaggedMetric)) {
				continue;
			}

			if (!getBaseName(entry.getKey()).equals(name)) {
				continue;
			}

			TaggedMetric taggedMetric = (TaggedMetric) entry.getValue();
			boolean found = true;
			if (searchTags != null) {
				for (Map.Entry<String, String> tag : searchTags.entrySet()) {
					if (!taggedMetric.getTags().entrySet().contains(tag)) {
						found = false;
						break;
					}
				}
				if (found) {
					context.stop();
					return taggedMetric;
				}
			} else if (taggedMetric.getTags() == null) {
				// search tags and metric tags are both null
				context.stop();
				return taggedMetric;
			}
		}
		context.stop();
		return null;
	}

	public static String getTaggedName(final String name,
			final Map<String, String> tags) {
		String taggedHashCode = "0";
		if (tags != null) {
			taggedHashCode = String.valueOf(tags.hashCode());
		}
		return name + delimiter + taggedHashCode;
	}

	public static String getBaseName(final String name) {
		if (name.contains(delimiter)) {
			return name.split(Pattern.quote(delimiter))[0];
		}
		return name;
	}
}
