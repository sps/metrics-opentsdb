package com.github.sps.metrics;

import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class TaggedJmxAttributeGauge implements TaggedMetric {

    private Map<String, String> tags;

	public TaggedJmxAttributeGauge(ObjectName objectName, String attributeName, Map<String, String> tags) {
        this.tags = tags;
    }

    public TaggedJmxAttributeGauge(MBeanServer mBeanServer, ObjectName objectName, String attributeName, Map<String, String> tags) {
    	this.tags = tags;
    }
	
	public Map<String, String> getTags() {
		return tags;
	}
}
