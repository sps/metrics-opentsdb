package com.github.sps.metrics;

import com.codahale.metrics.Gauge;

public interface TaggedGauge<T> extends TaggedMetric, Gauge<T> {

}
