package com.github.sps.metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;

public abstract class TaggedCachedGauge<T> extends CachedGauge<T> implements TaggedGauge<T> {

	protected TaggedCachedGauge(Clock clock, long timeout, TimeUnit timeoutUnit) {
		super(clock, timeout, timeoutUnit);
	}
	
	protected TaggedCachedGauge(long timeout, TimeUnit timeoutUnit) {
		this(Clock.defaultClock(), timeout, timeoutUnit);
    }
}
