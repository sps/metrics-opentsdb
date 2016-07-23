package com.github.sps.metrics;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Gauge;

public class TaggedMetricTest {

	private TaggedMetric taggedMetric;
	private Map<String, String> tags;
	
	@Before
	public void setUp() throws Exception {
		tags = new HashMap<String, String>();
		tags.put("a", "b");
		tags.put("c", "d");
	}

	@Test
	public void testTaggedHistogram() {
		taggedMetric = new TaggedHistogram(null, tags);
		assertEquals(tags, taggedMetric.getTags());
	}
	
	@Test
	public void testTaggedCounter() {
		taggedMetric = new TaggedCounter(tags);
		assertEquals(tags, taggedMetric.getTags());
	}
	
	@Test
	public void testTaggedMeter() {
		taggedMetric = new TaggedMeter(tags);
		assertEquals(tags, taggedMetric.getTags());
	}
	
	@Test
	public void testTaggedTimer() {
		taggedMetric = new TaggedTimer(tags);
		assertEquals(tags, taggedMetric.getTags());
	}
	
	@Test
	public void testTaggedCachedGauge() {
		taggedMetric = new TaggedCachedGauge<Integer>(1, TimeUnit.DAYS) {
			@Override
			public Map<String, String> getTags() {
				return tags;
			}
			@Override
			protected Integer loadValue() {
				return 1;
			}
		};
		assertEquals(tags, taggedMetric.getTags());
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testTaggedDerivativeGauge() {
		taggedMetric = new TaggedDerivativeGauge<Integer, Integer>(mock(Gauge.class)) {

			@Override
			public Map<String, String> getTags() {
				// TODO Auto-generated method stub
				return tags;
			}

			@Override
			protected Integer transform(Integer value) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		assertEquals(tags, taggedMetric.getTags());
	}
	
	@Test
	public void testTaggedJmxAttributeGauge() throws MalformedObjectNameException {
		ObjectName name = mock(ObjectName.class);
		taggedMetric = new TaggedJmxAttributeGauge(name, "bar", tags);
		assertEquals(tags, taggedMetric.getTags());
		
		taggedMetric = new TaggedJmxAttributeGauge(mock(MBeanServer.class),  name,  "foo", tags);
		assertEquals(tags, taggedMetric.getTags());
	}
	
	@Test
	public void testTaggedRatioGauge() {
		taggedMetric = new TaggedRatioGauge() {
			
			@Override
			public Map<String, String> getTags() {
				// TODO Auto-generated method stub
				return tags;
			}
			
			@Override
			protected Ratio getRatio() {
				// TODO Auto-generated method stub
				return null;
			}
		};
		assertEquals(tags, taggedMetric.getTags());
	}

}
