package com.github.sps.metrics;

import com.codahale.metrics.DerivativeGauge;
import com.codahale.metrics.Gauge;

public abstract class TaggedDerivativeGauge<F, T> extends DerivativeGauge<F, T> implements TaggedGauge<T>{
	protected TaggedDerivativeGauge(Gauge<F> base) {
		super(base);
	}
}
