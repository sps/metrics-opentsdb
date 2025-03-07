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

import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.core.MediaType;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;

/**
 * OpenTSDB 2.0 jersey based REST client.
 *
 * <a href="http://opentsdb.net/docs/build/html/api_http/index.html#version-1-x-to-2-x">HTTP API</a>
 *
 * @author Sean Scanlon sean.scanlon@gmail.com
 */
public class OpenTsdb {

    public static final int DEFAULT_BATCH_SIZE_LIMIT = 0;
    public static final int CONN_TIMEOUT_DEFAULT_MS = 5000;
    public static final int READ_TIMEOUT_DEFAULT_MS = 5000;
    private static final Logger logger = LoggerFactory.getLogger(OpenTsdb.class);

    /**
     * Initiate a client Builder with the provided base opentsdb server url.
     *
     * @param baseUrl
     * @return a {@link Builder}
     */
    public static Builder forService(String baseUrl) {
        return new Builder(baseUrl);
    }

    /**
     * create a client by providing the underlying WebResource
     *
     * @param apiResource
     * @return
     */
    public static OpenTsdb create(WebTarget apiResource) {
        return new OpenTsdb(apiResource);
    }

    private final WebTarget apiResource;
    private int batchSizeLimit = DEFAULT_BATCH_SIZE_LIMIT;

    public static class Builder {

        private Integer connectionTimeout = CONN_TIMEOUT_DEFAULT_MS;
        private Integer readTimeout = READ_TIMEOUT_DEFAULT_MS;
        private final String baseUrl;
        private boolean gzipEnabled = false;

        public Builder(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public Builder withConnectTimeout(Integer connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder withReadTimeout(Integer readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder withGzipEnabled(boolean gzipEnabled) {
            this.gzipEnabled = gzipEnabled;
            return this;
        }

        public OpenTsdb create() {
            return new OpenTsdb(baseUrl, connectionTimeout, readTimeout, gzipEnabled);
        }
    }

	/**
	 * For OpenTsdbTelnet
	 */
	protected OpenTsdb() {
		this.apiResource = null;
	}

    private OpenTsdb(WebTarget apiResource) {
        this.apiResource = apiResource;
    }

    private OpenTsdb(String baseURL, Integer connectionTimeout, Integer readTimeout, boolean gzipEnabled) {
        ClientBuilder builder = ClientBuilder.newBuilder()
                                             .register(JacksonFeature.class);
        if (gzipEnabled) {
            builder = builder.register(GZipEncoder.class)
                             .register(EncodingFilter.class)
                             .property(ClientProperties.USE_ENCODING, "gzip");
        }
        final Client client = builder.build();
        client.property(ClientProperties.CONNECT_TIMEOUT, connectionTimeout);
        client.property(ClientProperties.READ_TIMEOUT, readTimeout);

        this.apiResource = client.target(baseURL);
    }

    public void setBatchSizeLimit(int batchSizeLimit) {
        this.batchSizeLimit = batchSizeLimit;
    }

    /**
     * Send a metric to opentsdb
     *
     * @param metric
     */
    public void send(OpenTsdbMetric metric) {
        send(Collections.singleton(metric));
    }

    /**
     * send a set of metrics to opentsdb
     *
     * @param metrics
     */
    public void send(Set<OpenTsdbMetric> metrics) {
        // we set the patch size because of existing issue in opentsdb where large batch of metrics failed
        // see at https://groups.google.com/forum/#!topic/opentsdb/U-0ak_v8qu0
        // we recommend batch size of 5 - 10 will be safer
        // alternatively you can enable chunked request
        if (batchSizeLimit > 0 && metrics.size() > batchSizeLimit) {
            final Set<OpenTsdbMetric> smallMetrics = new HashSet<OpenTsdbMetric>();
            for (final OpenTsdbMetric metric : metrics) {
                smallMetrics.add(metric);
                if (smallMetrics.size() >= batchSizeLimit) {
                    sendHelper(smallMetrics);
                    smallMetrics.clear();
                }
            }
            sendHelper(smallMetrics);
        } else {
            sendHelper(metrics);
        }
    }

    private void sendHelper(Set<OpenTsdbMetric> metrics) {
        /*
         * might want to bind to a specific version of the API.
         * according to: http://opentsdb.net/docs/build/html/api_http/index.html#api-versioning
         * "if you do not supply an explicit version, ... the latest version will be used."
         * circle back on this if it's a problem.
         */
        if (!metrics.isEmpty()) {
            try {
                final Entity<?> entity = Entity.entity(metrics, MediaType.APPLICATION_JSON);
                apiResource.path("/api/put").request().post(entity);
            } catch (Exception ex) {
                logger.error("send to opentsdb endpoint failed", ex);
            }
        }
    }

}
