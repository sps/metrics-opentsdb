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

        assertTrue(o1.equals(o1));

        assertFalse(o1.equals(new Object()));

        assertTrue(o1.equals(o2));
        assertTrue(o2.equals(o1));
        assertFalse(o1.equals(o3));
        assertFalse(o3.equals(o1));

        assertTrue(o1.hashCode() == o2.hashCode());
        assertFalse(o3.hashCode() == o2.hashCode());

        assertNotNull(o1.toString());

    }

    @Test
    public void testTagsInNameEncode() {
        String encoded = OpenTsdbMetric.encodeTagsInName("counter", "foo=bar");

        assertTrue(OpenTsdbMetric.hasEncodedTagInName(encoded));
        assertFalse(OpenTsdbMetric.hasEncodedTagInName("counter"));

        String prefixed = "prefix."+encoded;
        String fixedPrefixed = OpenTsdbMetric.fixEncodedTagsInNameAfterPrefix(prefixed);

        assertTrue(OpenTsdbMetric.hasEncodedTagInName(fixedPrefixed));

        String appended = encoded+".app";
        assertTrue(OpenTsdbMetric.hasEncodedTagInName(appended));
    }

    @Test
    public void testTags() {
        OpenTsdbMetric o1 = OpenTsdbMetric.named(OpenTsdbMetric.encodeTagsInName("counter", "foo=bar"))
                .withValue(1L)
                .withTimestamp(null)
                .withTags(null)
                .build();

        assertTrue(o1.equals(o1));

        assertEquals("counter", o1.getMetric().toString());

        Map<String,String> tags = o1.getTags();
        assertEquals(1, tags.size());
        assertTrue(tags.containsKey("foo"));
        assertEquals("bar", tags.get("foo"));
    }

    @Test
    public void testTelnetString() {
        OpenTsdbMetric o1 = OpenTsdbMetric.named(OpenTsdbMetric.encodeTagsInName("counter", "foo=bar"))
                .withValue(1L)
                .withTimestamp(Long.valueOf(123))
                .build();

        String telnetString = o1.toTelnetPutString();
        assertEquals("put counter 123 1 foo=bar\n", telnetString);
    }

    @Test
    public void testSanitize() {

        assertEquals("foo_---", OpenTsdbMetric.sanitize("foo_*&^"));

        OpenTsdbMetric o1 = OpenTsdbMetric.named(OpenTsdbMetric.encodeTagsInName("counter!@#", "^&*foo=bar()&"))
                .withValue(1L)
                .withTimestamp(Long.valueOf(123))
                .build();

        String telnetString = o1.toTelnetPutString();
        assertEquals("put counter--- 123 1 ---foo=bar---\n", telnetString);
    }
}
