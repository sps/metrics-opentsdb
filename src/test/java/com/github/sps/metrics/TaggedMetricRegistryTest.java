package com.github.sps.metrics;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;

public class TaggedMetricRegistryTest {
	
	private TaggedMetricRegistry registry;
	private Map<String, String> tags;

	@Before
	public void setUp() throws Exception {
		registry = new TaggedMetricRegistry();
		tags = new HashMap<String, String>();
	}

	@Test
	public void testTaggedCounter() {
		tags.put("a", "b");
		String name = "foo";
		TaggedCounter counter = registry.taggedCounter(name, tags);
		assertEquals(1, registry.getCounters().size());
		String expected = name + TaggedMetricRegistry.delimiter + tags.hashCode();
		for(Map.Entry<String, Counter> entry : registry.getCounters().entrySet()) {
			assertEquals(expected, entry.getKey());
			assertEquals(counter, entry.getValue());
			TaggedCounter actual = (TaggedCounter) entry.getValue();
			assertEquals(tags, actual.getTags());
		}
	}

	@Test
	public void testGetTaggedCounter() {
		tags.put("a", "b");
		tags.put("c", "d");
		String name = "foo";
		TaggedCounter counter = registry.taggedCounter(name, tags);
		
		Map<String, String> searchTags = new HashMap<String, String>();
		searchTags.put("a", "b");
		TaggedCounter actual = registry.getTaggedCounter("foo", searchTags);
		assertEquals(counter, actual);
	}
	
	
	@Test
	public void testGetTaggedMeter() {
		tags.put("a", "b");
		tags.put("c", "d");
		String name = "foo";
		TaggedMeter counter = registry.taggedMeter(name, tags);
		
		Map<String, String> searchTags = new HashMap<String, String>();
		searchTags.put("a", "b");
		TaggedMeter actual = registry.getTaggedMeter("foo", searchTags);
		assertEquals(counter, actual);
	}
	
	@Test
	public void testTaggedMeter() {
		tags.put("a", "b");
		String name = "foo";
		TaggedMeter counter = registry.taggedMeter(name, tags);
		assertEquals(1, registry.getMeters().size());
		String expected = name + TaggedMetricRegistry.delimiter + tags.hashCode();
		for(Map.Entry<String, Meter> entry : registry.getMeters().entrySet()) {
			assertEquals(expected, entry.getKey());
			assertEquals(counter, entry.getValue());
			TaggedMeter actual = (TaggedMeter) entry.getValue();
			assertEquals(tags, actual.getTags());
		}
	}

	@Test
	public void testGetTaggedHistogram() {
		tags.put("a", "b");
		tags.put("c", "d");
		String name = "foo";
		TaggedHistogram counter = registry.taggedHistogram(mock(Reservoir.class), name, tags);
		
		Map<String, String> searchTags = new HashMap<String, String>();
		searchTags.put("a", "b");
		TaggedHistogram actual = registry.getTaggedHistogram("foo", searchTags);
		assertEquals(counter, actual);
	}
	
	@Test
	public void testTaggedHistogram() {
		tags.put("a", "b");
		String name = "foo";
		TaggedHistogram counter = registry.taggedHistogram(mock(Reservoir.class), name, tags);
		assertEquals(1, registry.getHistograms().size());
		String expected = name + TaggedMetricRegistry.delimiter + tags.hashCode();
		for(Map.Entry<String, Histogram> entry : registry.getHistograms().entrySet()) {
			assertEquals(expected, entry.getKey());
			assertEquals(counter, entry.getValue());
			TaggedHistogram actual = (TaggedHistogram) entry.getValue();
			assertEquals(tags, actual.getTags());
		}
	}
	
	@Test
	public void testGetTaggedTimer() {
		tags.put("a", "b");
		tags.put("c", "d");
		String name = "foo";
		TaggedTimer counter = registry.taggedTimer(name, tags);
		
		Map<String, String> searchTags = new HashMap<String, String>();
		searchTags.put("a", "b");
		TaggedTimer actual = registry.getTaggedTimer("foo", searchTags);
		assertEquals(counter, actual);
	}
	
	@Test
	public void testTaggedTimer() {
		tags.put("a", "b");
		String name = "foo";
		TaggedTimer timer = registry.taggedTimer(name, tags);
		
		String expected = name + TaggedMetricRegistry.delimiter + tags.hashCode();
		TaggedTimer actual = registry.getTaggedTimer("foo", tags);
		assertEquals(timer, actual);
		assertEquals(timer.getTags(), actual.getTags());
	}
	
	@Test
	public void testGetTaggedMetricNonTaggedMetric() {
		registry.meter("foo");
		assertEquals(null, registry.getTaggedMetric(null, null));
		assertEquals(null, registry.getTaggedMetric("foo", null));
	}
	
	@Test
	public void testGetTaggedMetricNull() {
		TaggedCounter expected = registry.taggedCounter("foo", null);
		assertEquals(expected, registry.getTaggedMetric("foo", null));
	}
	
	@Test
	public void testGetTaggedMetricNull2() {
		registry.taggedCounter("foo", tags);
		assertEquals(null, registry.getTaggedMetric("foo", null));
	}
	
	@Test
	public void testGetTaggedMetric() {
		Map<String, String> tags1 = new HashMap<String, String>();
		tags1.put("1", "2");
		registry.taggedCounter("hello", tags1);
		
		tags.put("a", "b");
		tags.put("c", "d");
		TaggedCounter expected = registry.taggedCounter("foo", tags);
		assertEquals(null, registry.getTaggedMetric("dne", tags));
		assertEquals(expected, registry.getTaggedMetric("foo", tags));
		
		Map<String, String> searchTags = new HashMap<String, String>();
		searchTags.put("a", "b");
		assertEquals(expected, registry.getTaggedMetric("foo", searchTags));
		
		searchTags = new HashMap<String, String>();
		searchTags.put("c", "d");
		assertEquals(expected, registry.getTaggedMetric("foo", searchTags));
		
		searchTags = new HashMap<String, String>();
		searchTags.put("a", "b");
		searchTags.put("c", "d");
		searchTags.put("1", "2");
		assertEquals(null, registry.getTaggedMetric("foo", searchTags));
	}

	@Test
	public void testGetTaggedName() {
		tags.put("a", "b");
		String actual = TaggedMetricRegistry.getTaggedName("foo", tags);
		String expected = "foo" + TaggedMetricRegistry.delimiter + tags.hashCode();
		assertEquals(actual, expected);
		
		tags = null;
		actual = TaggedMetricRegistry.getTaggedName("foo", tags);
		expected = "foo" + TaggedMetricRegistry.delimiter + "0";
		assertEquals(actual, expected);
	}

	@Test
	public void testGetBaseName() {
		String name = "foo" + TaggedMetricRegistry.delimiter + "bar";
		assertEquals("foo", TaggedMetricRegistry.getBaseName(name));
		assertEquals("123", TaggedMetricRegistry.getBaseName("123"));
	}
	
	@Test
	public void testRegisterTaggedMetric() {
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("a", "b");
		TaggedMetric metric = new TaggedTimer(tags);
		TaggedMetric actual = registry.getOrRegisterTaggedMetric("foo", metric);
		assertEquals(metric, actual);
		actual = registry.getOrRegisterTaggedMetric("foo", metric);
		assertEquals(metric, actual);
	}

}

