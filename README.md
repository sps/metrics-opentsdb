Metrics OpenTSDB  [![Build Status](https://travis-ci.org/sps/metrics-opentsdb.png?branch=master)](https://travis-ci.org/sps/metrics-opentsdb) [![Coverage Status](https://coveralls.io/repos/sps/metrics-opentsdb/badge.png?branch=master)](https://coveralls.io/r/sps/metrics-opentsdb?branch=master)
================
A Coda Hale [Metrics](http://metrics.codahale.com/) Reporter.

OpenTsdbReporter allows your application to constantly stream metric values to an opentsdb server
via the [2.0 HTTP API](http://opentsdb.net/docs/build/html/api_http/index.html).

Example Usage
-------------

[dropwizard](http://dropwizard.io/) 0.7.x app:

    @Override
    public void run(T configuration, Environment environment) throws Exception {
    ...
      OpenTsdbReporter.forRegistry(environment.metrics())
          .prefixedWith("app_name")
          .withTags(ImmutableMap.of("other", "tags"))
          .build(OpenTsdb.forService("http://opentsdb/")
          .create())
          .start(30L, TimeUnit.SECONDS);



