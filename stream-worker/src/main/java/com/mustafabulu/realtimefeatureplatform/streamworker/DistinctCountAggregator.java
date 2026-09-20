package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import org.springframework.stereotype.Component;

@Component
class DistinctCountAggregator implements Aggregator {

    @Override
    public AggregationType type() {
        return AggregationType.DISTINCT_COUNT;
    }

    @Override
    public Number aggregate(AggregationInput input) {
        Object value = EventFieldResolver.resolve(input.event(), input.definition().valueField())
                .orElseThrow(() -> new IllegalArgumentException(
                        "missing valueField " + input.definition().valueField()));
        DistinctCountState next = DistinctCountState.parse(input.stateStore().get(input.stateKey()))
                .add(value);
        input.stateStore().put(input.stateKey(), next.serialize());
        return next.count();
    }
}
