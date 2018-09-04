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
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Sean Scanlon <sean.scanlon@gmail.com>
 */
public class OpenTsdbMetricTest {

    @Test
    public void testEquals() {
        OpenTsdbMetric o1 = OpenTsdbMetric.named(null)
                .withValue(1L)
                .withTimestamp(null)
                .withTags(null)
                .build();

        OpenTsdbMetric o2 = OpenTsdbMetric.named(null)
                .withValue(1L)
                .withTimestamp(null)
                .withTags(null)
                .build();

        OpenTsdbMetric o3 = OpenTsdbMetric.named(null)
                .withValue(1L)
                .withTimestamp(null)
                .withTags(Collections.singletonMap("foo", "bar"))
                .build();

        assertEquals(o1, o1);

        assertNotEquals(o1, new Object());

        assertEquals(o1, o2);
        assertEquals(o2, o1);
        assertNotEquals(o1, o3);
        assertNotEquals(o3, o1);

        assertEquals(o1.hashCode(), o2.hashCode());
        assertNotEquals(o3.hashCode(), o2.hashCode());

        assertNotNull(o1.toString());

    }

    @Test
    public void testTags() {
        OpenTsdbMetric o1 = OpenTsdbMetric.tagged(MetricName.build("counter").tagged("foo", "bar"))
                .withValue(1L)
                .withTimestamp(null)
                .withTags(null)
                .build();

        assertEquals(o1, o1);

        assertEquals("counter", o1.getMetric());

        Map<String,String> tags = o1.getTags();
        assertEquals(1, tags.size());
        assertTrue(tags.containsKey("foo"));
        assertEquals("bar", tags.get("foo"));
    }

    @Test
    public void testTelnetString() {
        OpenTsdbMetric o1 = OpenTsdbMetric.tagged(MetricName.build("counter").tagged("foo", "bar"))
                .withValue(1L)
                .withTimestamp(123L)
                .build();

        String telnetString = o1.toTelnetPutString();
        assertEquals("put counter 123 1 foo=bar\n", telnetString);
    }

    @Test
    public void testSanitize() {

        assertEquals("foo_---", OpenTsdbMetric.sanitize("foo_*&^"));

        OpenTsdbMetric o1 = OpenTsdbMetric.tagged(MetricName.build("counter!@#").tagged("^&*foo", "bar()&"))
                .withValue(1L)
                .withTimestamp(123L)
                .build();

        String telnetString = o1.toTelnetPutString();
        assertEquals("put counter--- 123 1 ---foo=bar---\n", telnetString);
    }
}
