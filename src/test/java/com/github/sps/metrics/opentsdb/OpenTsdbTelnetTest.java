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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * @author Adam Lugowski <adam.lugowski@turn.com>
 */
@RunWith(MockitoJUnitRunner.class)
public class OpenTsdbTelnetTest {

	private OpenTsdbTelnet openTsdb;

	@Mock
	private Writer mockWriter;

	private StringWriter writer;

	@Before
	public void setUp() {
		writer = new StringWriter();
		openTsdb = OpenTsdbTelnet.forWriter(writer).create();
	}

	@Test
	public void testSend() {
		OpenTsdbMetric o1 = OpenTsdbMetric.tagged(MetricName.build("counter").tagged("foo", "bar"))
				.withValue(1L)
				.withTimestamp(123L)
				.build();

		openTsdb.send(o1);

		String telnetString = writer.toString();
		assertEquals("put counter 123 1 foo=bar\n", telnetString);

	}

	@Test
	public void testSendMultiple() {
		Set<OpenTsdbMetric> metrics = new HashSet<>();

		for (int i = 0; i < 10; i++) {
			OpenTsdbMetric o1 = OpenTsdbMetric.tagged(MetricName.build("counter").tagged("foo", "bar" + i))
					.withValue(1L)
					.withTimestamp(123L)
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
	public void testSendEmpty() {
		openTsdb = OpenTsdbTelnet.forWriter(mockWriter).create();
		openTsdb.send(Collections.<OpenTsdbMetric>emptySet());
		verifyZeroInteractions(mockWriter);
	}

	@Test
	public void testBuilder() {
		assertNotNull(OpenTsdbTelnet.forService("localhost", 123)
				.create());

		assertNotNull(OpenTsdbTelnet.forWriter(new StringWriter())
				.create());
	}

	@Test
	public void testSwallowsExceptionsOnWrite() throws IOException {
		// We only log the exceptions when our underlying writer throws an IOException
		final Writer mockWriter = Mockito.mock(Writer.class);
		doThrow(new IOException("Exception through write")).when(mockWriter).write(Mockito.anyString());

		openTsdb = OpenTsdbTelnet.forWriter(mockWriter).create();
		OpenTsdbMetric o1 = OpenTsdbMetric.tagged(MetricName.build("counter").tagged("foo", "bar"))
				.withValue(1L)
				.withTimestamp(123L)
				.build();
		openTsdb.send(o1);
		verify(mockWriter).close();
	}

	@Test
	public void testSwallowsExceptionsOnClose() throws IOException {
		// We only log the exceptions when our underlying writer throws an IOException
		doNothing().when(mockWriter).write(Mockito.anyString());
		doThrow(new IOException("Exception while closing")).when(mockWriter).close();

		openTsdb = OpenTsdbTelnet.forWriter(mockWriter).create();
		OpenTsdbMetric o1 = OpenTsdbMetric.tagged(MetricName.build("counter").tagged("foo", "bar"))
				.withValue(1L)
				.withTimestamp(123L)
				.build();
		openTsdb.send(o1);

		verify(mockWriter).close();
	}
}
