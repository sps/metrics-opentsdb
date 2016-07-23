Metrics OpenTSDB  [![Build Status](https://travis-ci.org/sps/metrics-opentsdb.png?branch=master)](https://travis-ci.org/sps/metrics-opentsdb) [![Coverage Status](https://coveralls.io/repos/sps/metrics-opentsdb/badge.png?branch=master)](https://coveralls.io/r/sps/metrics-opentsdb?branch=master)
================
A Coda Hale [Metrics](http://metrics.codahale.com/) Reporter.

OpenTsdbReporter allows your application to constantly stream metric values to an opentsdb server
via the [2.0 HTTP API](http://opentsdb.net/docs/build/html/api_http/index.html).

Example Usage
-------------

[dropwizard](http://dropwizard.io/) 0.8.x app:

    @Override
    public void run(T configuration, Environment environment) throws Exception {
    ...
      OpenTsdbReporter.forRegistry(environment.metrics())
          .prefixedWith("app_name")
          .withTags(ImmutableMap.of("other", "tags"))
          .build(OpenTsdb.forService("http://opentsdb/")
          .create())
          .start(30L, TimeUnit.SECONDS);
          

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


    
	




