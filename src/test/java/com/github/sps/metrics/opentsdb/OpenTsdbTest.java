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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * @author Sean Scanlon <sean.scanlon@gmail.com>
 */
@RunWith(MockitoJUnitRunner.class)
public class OpenTsdbTest {

    private OpenTsdb openTsdb;

    @Mock
    private WebResource apiResource;

    @Mock
    WebResource.Builder mockBuilder;

    @Before
    public void setUp() {
        openTsdb = OpenTsdb.create(apiResource);
        openTsdb.setBatchSizeLimit(10);
    }

    @Test
    public void testSend() {
        when(apiResource.path("/api/put")).thenReturn(apiResource);
        when(apiResource.type(MediaType.APPLICATION_JSON)).thenReturn(mockBuilder);
        when(mockBuilder.entity(anyObject())).thenReturn(mockBuilder);
        openTsdb.send(OpenTsdbMetric.named("foo").build());
        verify(mockBuilder).post();
    }

    @Test
    public void testSendMultiple() {
        when(apiResource.path("/api/put")).thenReturn(apiResource);
        when(apiResource.type(MediaType.APPLICATION_JSON)).thenReturn(mockBuilder);
        when(mockBuilder.entity(anyObject())).thenReturn(mockBuilder);

        Set<OpenTsdbMetric> metrics = new HashSet<OpenTsdbMetric>(Arrays.asList(OpenTsdbMetric.named("foo").build()));
        openTsdb.send(metrics);
        verify(mockBuilder, times(1)).post();

        // split into two request
        for (int i = 1; i < 10; i++) {
            metrics.add(OpenTsdbMetric.named("foo").build());
        }
        openTsdb.send(metrics);
        verify(mockBuilder, times(2)).post();
    }

    @Test
    public void testBuilder() {
        assertNotNull(OpenTsdb.forService("foo")
                .withReadTimeout(1)
                .withConnectTimeout(1)
                .create());
    }

}
