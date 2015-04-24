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

import com.sun.jersey.api.client.WebResource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.ws.rs.core.MediaType;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * @author Adam Lugowski <adam.lugowski@turn.com>
 */
@RunWith(MockitoJUnitRunner.class)
public class OpenTsdbTelnetTest {

  private OpenTsdbTelnet openTsdb;

  private StringWriter writer;

  @Before
  public void setUp() {
    writer = new StringWriter();
    openTsdb = OpenTsdbTelnet.forWriter(writer).create();
  }

  @Test
  public void testSend() {
    OpenTsdbMetric o1 = OpenTsdbMetric.named(OpenTsdbMetric.encodeTagsInName("counter", "foo=bar"))
        .withValue(1L)
        .withTimestamp(Long.valueOf(123))
        .build();

    openTsdb.send(o1);

    String telnetString = writer.toString();
    assertEquals("put counter 123 1 foo=bar\n", telnetString);

  }

  @Test
  public void testSendMultiple() {
    Set<OpenTsdbMetric> metrics = new HashSet<OpenTsdbMetric>();

    for (int i = 0; i < 10; i++) {
      OpenTsdbMetric o1 = OpenTsdbMetric.named(OpenTsdbMetric.encodeTagsInName("counter", "foo=bar"+i))
          .withValue(1L)
          .withTimestamp(Long.valueOf(123))
          .build();

      metrics.add(o1);
    }
    openTsdb.send(metrics);

    // verify output
    String telnetString = writer.toString();
    String[] lines = telnetString.split("\n");

    Arrays.sort(lines); // necessary because a HashSet doesn't guarantee an iteration order

    assertEquals(10, lines.length);
    for (int i = 0; i < 10; i++) {
      assertEquals("put counter 123 1 foo=bar" + i, lines[i]);
    }
  }

  @Test
  public void testBuilder() {
    assertNotNull(OpenTsdbTelnet.forService("localhost", 123)
        .create());

    assertNotNull(OpenTsdbTelnet.forWriter(new StringWriter())
        .create());
  }

}
