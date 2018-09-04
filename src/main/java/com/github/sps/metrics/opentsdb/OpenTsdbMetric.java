/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.sps.metrics.opentsdb;

import io.dropwizard.metrics5.MetricName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a metric.
 *
 * @author Sean Scanlon <sean.scanlon@gmail.com>
 * @author Adam Lugowski <adam.lugowski@turn.com>
 *
 */
public class OpenTsdbMetric {

    private OpenTsdbMetric() {}

    /**
     * Convert a tag map into a space-delimited string.
     *
     * @param tagMap
     * @return a space-delimited string of key-value pairs. For example, {@code "key1=value1 key_n=value_n"}
     */
    public static String formatTags(final Map<String, String> tagMap) {
        StringBuilder stringBuilder = new StringBuilder();
        String delimeter = "";

        for (Map.Entry<String, String> tag : tagMap.entrySet()) {
            stringBuilder.append(delimeter)
                    .append(sanitize(tag.getKey()))
                    .append("=")
                    .append(sanitize(tag.getValue()));
            delimeter = " ";
        }

        return stringBuilder.toString();
    }

    /**
     * Creates a Builder for a tagged metric.
     *
     * @return a {@link Builder}
     */
    public static Builder tagged(MetricName name) {
        // TODO: better null support
        if (name != null)
            return new Builder(name.getKey()).withTags(name.getTags());
        else
            return new Builder(null);
    }

    /**
     * Creates a Builder for a simple metric (name only, no tags).
     *
     * @return a {@link Builder}
     */
    public static Builder named(String name) {
        return new Builder(name);
    }

    private String metric;

    private Long timestamp;

    private Object value;

    private Map<String, String> tags = new HashMap<>();

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (!(o instanceof OpenTsdbMetric)) {
            return false;
        }

        final OpenTsdbMetric rhs = (OpenTsdbMetric) o;

        return equals(metric, rhs.metric)
                && equals(timestamp, rhs.timestamp)
                && equals(value, rhs.value)
                && equals(tags, rhs.tags);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{metric, timestamp, value, tags});
    }

    public static class Builder {

        private final OpenTsdbMetric metric;

        public Builder(String name) {
            this.metric = new OpenTsdbMetric();
            metric.metric = name;
        }

        public OpenTsdbMetric build() {
            return metric;
        }

        public Builder withValue(Object value) {
            metric.value = value;
            return this;
        }

        public Builder withTimestamp(Long timestamp) {
            metric.timestamp = timestamp;
            return this;
        }

        public Builder withTags(Map<String, String> tags) {
            if (tags != null) {
                metric.tags.putAll(tags);
            }
            return this;
        }
    }


    /**
     * Returns a JSON string version of this metric compatible with the HTTP API reporter.
     *
     * Example:
     * <pre><code>
     * {
     *     "metric": "sys.cpu.nice",
     *     "timestamp": 1346846400,
     *     "value": 18,
     *     "tags": {
     *         "host": "web01",
     *         "dc": "lga"
     *     }
     * }
     * </code></pre>
     * @return a JSON string version of this metric compatible with the HTTP API reporter.
     */
    @Override
    public String toString() {
        return this.getClass().getSimpleName()
                + "->metric: " + metric
                + ",value: " + value
                + ",timestamp: " + timestamp
                + ",tags: " + tags;
    }

    /**
     * Sanitizes a metric name, tag key, or tag value by removing characters not allowed by TSDB.
     *
     * Supported characters are {@code a-z A-Z 0-9 - _ . / }
     *
     * @param name a metric name, tag key, or tag value
     * @return {@code name} where unsupported characters are replaced with {@code "-"}.
     */
	public static String sanitize(String name) {
		return name.replaceAll("[^a-zA-Z0-9\\-_./]", "-");
	}

    /**
     * Returns a put string version of this metric compatible with the telnet-style reporter.
     *
     * Format:
     * <pre><code>
     * put (metric-name) (timestamp) (value) (tags)
     * </code></pre>
     *
     * Example:
     * <pre><code>
     * put sys.cpu.nice 1346846400 18 host=web01 dc=lga
     * </code></pre>
     *
     * @return a string version of this metric compatible with the telnet reporter.
     */
	public String toTelnetPutString() {
		String tagString = formatTags(tags);

		return String.format("put %s %d %s %s%n", sanitize(metric), timestamp, value, tagString);
	}

    public String getMetric() {
        return metric;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    private boolean equals(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }
}
