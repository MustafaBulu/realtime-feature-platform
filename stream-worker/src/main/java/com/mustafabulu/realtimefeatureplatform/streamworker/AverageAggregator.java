package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import org.springframework.stereotype.Component;

@Component
class AverageAggregator implements Aggregator {

    @Override
    public AggregationType type() {
        return AggregationType.AVG;
    }

    @Override
    public Number aggregate(AggregationInput input) {
        Object value = EventFieldResolver.resolve(input.event(), input.definition().valueField())
                .orElseThrow(() -> new IllegalArgumentException(
                        "missing valueField " + input.definition().valueField()));
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("valueField must resolve to a number");
        }

        AverageState next = AverageState.parse(input.stateStore().get(input.stateKey()))
                .add(number.doubleValue());
        input.stateStore().put(input.stateKey(), next.serialize());
        return next.value();
    }
}
