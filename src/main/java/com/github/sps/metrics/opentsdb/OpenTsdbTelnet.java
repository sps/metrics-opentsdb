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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.Collections;
import java.util.Set;

/**
 * Same as the {@link OpenTsdb} class in this package, but uses the
 * {@link <a href="http://opentsdb.net/docs/build/html/user_guide/writing.html#telnet">Telnet</a>}
 * format.
 *
 * This class can write to a {@link Socket} or a {@link Writer}.
 *
 * @author Adam Lugowski <adam.lugowski@turn.com>
 */
public class OpenTsdbTelnet extends OpenTsdb {
  private static final Logger logger = LoggerFactory.getLogger(OpenTsdbTelnet.class);

  protected interface WriterFactory {
    Writer getWriter() throws java.io.IOException;
  }

  protected static class SingleWriterFactory implements WriterFactory {
    private Writer writer;

    public SingleWriterFactory(Writer writer) {
      this.writer = writer;
    }

    @Override
    public Writer getWriter() {
      return writer;
    }
  }

  protected static class SocketWriterFactory implements WriterFactory {
    private String host = null;
    private int port = -1;

    public SocketWriterFactory(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public Writer getWriter() throws java.io.IOException {
      Socket socket = new Socket(host, port, null, 0);
      Writer socketWriter = new OutputStreamWriter(socket.getOutputStream());
      return new BufferedWriter(socketWriter);
    }
  }

  /**
   * Initiate a client {@link Builder} with the provided opentsdb server {@code host:port}.
   *
   * @param host is the hostname of the opentsdb server
   * @param port is the port
   * @return a {@link Builder}
   */
  public static Builder forService(String host, int port) {
    return new Builder(new SocketWriterFactory(host, port));
  }

  /**
   * Initiate a client {@link Builder} with a particular {@link Writer}.
   *
   * @param writer {@link Writer} to write metrics to. Will be closed when done.
   * @return a {@link Builder}
   */
  public static Builder forWriter(Writer writer) {
    return new Builder(new SingleWriterFactory(writer));
  }

  private WriterFactory writerFactory;

  public static class Builder {
    private WriterFactory writerFactory;

    private Builder(WriterFactory writerFactory) {
      this.writerFactory = writerFactory;
    }

    public OpenTsdbTelnet create() {
      return new OpenTsdbTelnet(writerFactory);
    }
  }

  private OpenTsdbTelnet(WriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

  /**
   * Send a metric to opentsdb
   *
   * @param metric
   */
  @Override
  public void send(OpenTsdbMetric metric) {
    send(Collections.singleton(metric));
  }

  /**
   * send a set of metrics to opentsdb
   *
   * @param metrics
   */
  @Override
  public void send(Set<OpenTsdbMetric> metrics) {
    if (metrics.isEmpty())
      return;

    Writer writer = null;
    try {
      writer = this.writerFactory.getWriter();
      write(metrics, writer);
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error writing codahale metrics", e);
      } else {
        logger.warn("Error writing codahale metrics: {}", e.getMessage());
      }
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          logger.error("Error while closing writer:", e);
        }
      }
    }
  }

  public void write(Set<OpenTsdbMetric> metrics, Writer writer) throws IOException {
    for (final OpenTsdbMetric metric : metrics) {
      writer.write(metric.toTelnetPutString());
    }
  }
}
