package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import org.springframework.stereotype.Component;

@Component
class CountAggregator implements Aggregator {

    @Override
    public AggregationType type() {
        return AggregationType.COUNT;
    }

    @Override
    public Number aggregate(AggregationInput input) {
        long next = NumericStateFormat.readLong(input.stateStore().get(input.stateKey())) + 1L;
        input.stateStore().put(input.stateKey(), Long.toString(next));
        return next;
    }
}
