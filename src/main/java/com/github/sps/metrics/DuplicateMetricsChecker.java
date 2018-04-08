package com.github.sps.metrics;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


public class DuplicateMetricsChecker implements IDuplicateMetricsChecker {
    private Logger logger = LoggerFactory.getLogger(DuplicateMetricsChecker.class.getName());

    private final Cache<String, Long> dupDuplicateMetrics;

    private final Funnel<Map<String, String>> mapFunnel = new Funnel<Map<String, String>>() {
        @Override
        public void funnel(Map<String, String> from, PrimitiveSink into) {
            for (Map.Entry<String, String> entry : from.entrySet()) {
                into.putString(entry.getKey(), Charsets.UTF_8);
                into.putString(entry.getValue(), Charsets.UTF_8);
            }
        }
    };

    public DuplicateMetricsChecker(long maxCapacity, int ttl) {
        this.dupDuplicateMetrics = CacheBuilder.newBuilder()
               .maximumSize(maxCapacity)
               .expireAfterWrite(ttl, TimeUnit.MINUTES).build();
    }

    protected boolean isDuplicate(String key, Map<String, String> tagsToUse, Callable<Long> callableLong) {
        final Hasher hc = Hashing.murmur3_128().newHasher().putString(key, Charsets.UTF_8);
        if (!tagsToUse.isEmpty()) {
            hc.putObject(tagsToUse, mapFunnel);
        }
        final String hashKey = hc.hash().toString();
        final Long prevMetric = dupDuplicateMetrics.getIfPresent(hashKey);
        try {
            final Long aLong = callableLong.call();
            if (aLong != -1L && aLong.equals(prevMetric))  {
                return true;
            }
            dupDuplicateMetrics.put(hashKey, aLong);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return false;
    }
    
    @Override
    public boolean isDuplicate(String key, final Counting metric, Map<String, String> tagsToUse) {
        return isDuplicate(key, tagsToUse, new Callable<Long>() {
            @Override
            public Long call() {
                return metric.getCount();
            }
        });
    }
    
    @Override
    public boolean isDuplicate(String key, final Gauge metric, Map<String, String> tagsToUse) {
        return isDuplicate(key, tagsToUse, new Callable<Long>() {
            @Override
            public Long call() {
                if (metric.getValue() instanceof Number) {
                    return ((Number) metric.getValue()).longValue();
                }
                logger.warn("UnSupport Type: {}", metric);
                return -1L;
            }
        });
    }

}


