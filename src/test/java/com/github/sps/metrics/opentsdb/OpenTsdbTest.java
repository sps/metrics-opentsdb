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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * @author Sean Scanlon <sean.scanlon@gmail.com>
 */
@RunWith(MockitoJUnitRunner.class)
public class OpenTsdbTest {

    private OpenTsdb openTsdb;

    @Mock
    private WebTarget apiResource;

    @Mock
    private Response response;

    @Mock
    Invocation.Builder mockBuilder;

    @Before
    public void setUp() {
        openTsdb = OpenTsdb.create(apiResource);
        openTsdb.setBatchSizeLimit(10);
    }

    @Test
    public void testSend() {
        when(response.getStatus()).thenReturn(OpenTsdb.API_PUT_SUCCESS_RESPONSE_CODE);
        when(apiResource.path("/api/put")).thenReturn(apiResource);
        when(apiResource.request()).thenReturn(mockBuilder);
        when(mockBuilder.post((Entity<?>) anyObject())).thenReturn(response);
        openTsdb.send(OpenTsdbMetric.named("foo").build());
        verify(mockBuilder).post((Entity<?>) anyObject());
    }

    @Test
    public void testSendMultiple() {
        when(response.getStatus()).thenReturn(OpenTsdb.API_PUT_SUCCESS_RESPONSE_CODE);
        when(apiResource.path("/api/put")).thenReturn(apiResource);
        when(apiResource.request()).thenReturn(mockBuilder);
        when(mockBuilder.post((Entity<?>) anyObject())).thenReturn(response);

        Set<OpenTsdbMetric> metrics = new HashSet<OpenTsdbMetric>(Arrays.asList(OpenTsdbMetric.named("foo").build()));
        openTsdb.send(metrics);
        verify(mockBuilder, times(1)).post((Entity<?>) anyObject());

        // split into two request
        for (int i = 1; i < 20; i++) {
            metrics.add(OpenTsdbMetric.named("foo" + i).build());
        }
        openTsdb.send(metrics);
        verify(mockBuilder, times(3)).post((Entity<?>) anyObject());
    }

    @Test
    public void testBuilder() {
        assertNotNull(OpenTsdb.forService("foo")
                .withReadTimeout(1)
                .withConnectTimeout(1)
                .withGzipEnabled(true)
                .create());
    }

    @Test
    public void testSendWithExceptionFromRequestSwallowed() {
        when(apiResource.path("/api/put")).thenReturn(apiResource);
        when(apiResource.request()).thenReturn(mockBuilder);
        when(mockBuilder.post((Entity<?>) anyObject())).thenThrow(new ProcessingException("Exception from underlying jersey client"));
        openTsdb.send(OpenTsdbMetric.named("foo").build());
        verify(mockBuilder).post((Entity<?>) anyObject());
    }

}
