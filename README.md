Metrics OpenTSDB  [![Build Status](https://travis-ci.org/sps/metrics-opentsdb.png?branch=master)](https://travis-ci.org/sps/metrics-opentsdb) [![Coverage Status](https://coveralls.io/repos/sps/metrics-opentsdb/badge.png?branch=master)](https://coveralls.io/r/sps/metrics-opentsdb?branch=master)
================
A Coda Hale [Metrics](http://metrics.codahale.com/) Reporter.

OpenTsdbReporter allows your application to constantly stream metric values to an opentsdb server
via the [2.0 HTTP API](http://opentsdb.net/docs/build/html/api_http/index.html) or the
[Telnet API](http://opentsdb.net/docs/build/html/user_guide/writing.html#telnet).

This reporter also supports per-metric tags in addition to a global set of tags.

Example Usage
-------------

[dropwizard](http://dropwizard.io/) 3.0.1 app:

    @Override
    public void run(T configuration, Environment environment) throws Exception {
    ...
        OpenTsdb opentsdb = OpenTsdb.forService("http://opentsdb/")
                                          .withGzipEnabled(true) // optional: compress requests to tsd
                                          .create();

        OpenTsdbReporter.forRegistry(environment.metrics())
                        .prefixedWith(environment.getName())
                        .withTags(ImmutableMap.of("other", "tags")) // static tags included with every metric
                        // .withBatchSize(10) // optional batching. unbounded by default. likely need to tune this.
                        .build(opentsdb)
                        .start(15L, TimeUnit.SECONDS); // tune your reporting interval


Tagged Metric Registry
----------------------

    // setup
    TaggedMetricRegistry metrics = new TaggedMetricRegistry();
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "localhost");
    tags.put("foo", "bar");

    OpenTsdbReporter.forRegistry(metrics)
        .withTags(tags)
        .withBatchSize(5)
		.build(OpenTsdb.forService("http://opentsdb/")
		.create())
		.start(30L, TimeUnit.SECONDS);

	// using metric with tags
	Map<String, String> counterTags = new HashMap<String, String>(tags);
	counterTags.put("trigger", trigger);

	TaggedCounter counter = metrics.taggedCounter("my.tagged.counter", counterTags);
	counter.inc();

* Completely backwords compatible with existing Coda Hale metrics
* All Coda Hale metrics have a Tagged\<metric\> counterpart (e.g. TaggedCounter, TaggedMeter, etc.)
* Registry can have default tags that can be overridden at the metric level
* Metrics can have additional tags not in the registry
* Calling a tagged\<metric\> function (e.g. taggedCounter(), taggedMeter(), etc.) on the TaggedMetric registry will perform a get or create operation.  If the same type of metric with the same name and tags is already registered in the registry, it will be returned, otherwise it will be created and returned.  There is no need to check for name or tag collisions.







The Telnet API is identical to the above, except with

    OpenTsdbTelnet.forService("mycollector.example.com", 4243)


For per-metric tags, encode the tags into the metric name using

    Map<String, String> myCounterTags;
    String name = OpenTsdbMetric.encodeTagsInName('mycounter', myCounterTags);
