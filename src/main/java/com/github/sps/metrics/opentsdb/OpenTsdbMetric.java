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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.lang.IllegalArgumentException;
import java.util.NoSuchElementException;

/**
 * Representation of a metric.
 *
 * @author Sean Scanlon <sean.scanlon@gmail.com>
 * @author Adam Lugowski <adam.lugowski@turn.com>
 *
 */
public class OpenTsdbMetric {

    private OpenTsdbMetric() {
    }

    /**
     * Convert a tag string into a tag map.
     *
     * @param tagString a space-delimited string of key-value pairs. For example, {@code "key1=value1 key_n=value_n"}
     * @return a tag {@link Map}
     * @throws IllegalArgumentException if the tag string is corrupted.
     */
    public static Map<String, String> parseTags(final String tagString) throws IllegalArgumentException {
        // delimit by whitespace or '='
        Scanner scanner = new Scanner(tagString).useDelimiter("\\s+|=");

        Map<String, String> tagMap = new HashMap<String, String>();
        try {
            while (scanner.hasNext()) {
                String tagName = scanner.next();
                String tagValue = scanner.next();
                tagMap.put(tagName, tagValue);
            }
        } catch (NoSuchElementException e) {
            // The tag string is corrupted.
            throw new IllegalArgumentException("Invalid tag string '" + tagString + "'");
        } finally {
            scanner.close();
        }

        return tagMap;
    }

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
     * Add TSDB tags to a CodaHale metric name.
     *
     * A CodaHale metric name is a single string, so there is no natural way to encode TSDB tags.
     * This function formats the tags into the metric name so they can later be parsed by the
     * Builder. The name is formatted such that it can be appended to create sub-metric names, as
     * happens with Meter and Histogram.
     *
     * @param name CodaHale metric name
     * @param tags A space-delimited string of TSDB key-value pair tags
     * @return A metric name encoded with tags.
     * @throws IllegalArgumentException if the tag string is invalid
     */
    public static String encodeTagsInName(final String name, final String tags) throws IllegalArgumentException {
        return encodeTagsInName(name, parseTags(tags));
    }

    /**
     * Add TSDB tags to a CodaHale metric name.
     *
     * A CodaHale metric name is a single string, so there is no natural way to encode TSDB tags.
     * This function formats the tags into the metric name so they can later be parsed by the
     * Builder. The name is formatted such that it can be appended to create sub-metric names, as
     * happens with Meter and Histogram.
     *
     * @param name CodaHale metric name
     * @param tags a {@link Map} of TSDB tags
     * @return A metric name encoded with tags.
     */
    public static String encodeTagsInName(final String name, final Map<String, String> tags) {
        return String.format("TAG(%s)%s", formatTags(tags), sanitize(name));
    }

    /**
     * Tests whether a name has been processed with {@code encodeTagsInName}.
     *
     * @param name a metric name
     * @return {@code true} if {@code name} has tags encoded, {@code false} otherwise.
     */
    public static boolean hasEncodedTagInName(final String name) {
        if (name == null)
            return false;

        return name.startsWith("TAG(");
    }

    /**
     * Call this function whenever a potentially tag-encoded name is prefixed.
     *
     * @param name a metric name with encoded tag strings that has been prefixed.
     * @return a fixed metric name
     */
    public static String fixEncodedTagsInNameAfterPrefix(final String name) {
        if (name == null)
            return name;

        int tagStart = name.indexOf("TAG(");

        if (tagStart == -1)
            return name; // no tags in this name

        if (tagStart == 0)
            return name; // tag string is already correct

        // extract the "TAG(...)" string from the middle of the name and put it at the front.
        int tagEnd = name.lastIndexOf(')');
        if (tagEnd == -1) {
            throw new IllegalArgumentException("Tag definition missing closing parenthesis for metric '" + name + "'");
        }

        String tagString = name.substring(tagStart, tagEnd+1);
        return tagString + name.substring(0, tagStart) + name.substring(tagEnd+1);
    }

    /**
     * Creates a Builder for a metric name.
     *
     * @param name name can contain either a pure CodaHale metric name, or a string returned by {@code encodeTagsInName}.
     *             If it's the latter, the tags are parsed out and passed to {@code withTags}.
     * @return a {@link Builder}
     */
    public static Builder named(String name) {
    /*
    A name can contain either a pure metric name, or a string returned by encodeTagsInName().
    If it's the latter, it looks like "TAG(tag1=value1 tag2=value2)metricname".
     */
    if (!hasEncodedTagInName(name)) {
            return new Builder(name);
        }

        // parse out the tags
        int tagEnd = name.lastIndexOf(')');
        if (tagEnd == -1) {
            throw new IllegalArgumentException("Tag definition missing closing parenthesis for metric '" + name + "'");
        }

        String tagString = name.substring(4, tagEnd);
        name = name.substring(tagEnd+1);

        return new Builder(name).withTags(parseTags(tagString));
    }

    private String metric;

    private Long timestamp;

    private Object value;

    private Map<String, String> tags = new HashMap<String, String>();

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
    return name.replaceAll("[^a-zA-Z0-9\\-\\_\\.\\/]", "-");
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

    return String.format("put %s %d %s %s\n", metric, timestamp, value, tagString);
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
