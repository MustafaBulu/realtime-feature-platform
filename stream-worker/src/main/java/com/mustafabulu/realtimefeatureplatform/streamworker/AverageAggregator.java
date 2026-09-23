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
        double weight = weight(input);

        AverageState next = AverageState.parse(input.stateStore().get(input.stateKey()))
                .add(number.doubleValue(), weight);
        input.stateStore().put(input.stateKey(), next.serialize());
        return next.value();
    }

    private static double weight(AggregationInput input) {
        if (input.definition().weightField() == null || input.definition().weightField().isBlank()) {
            return 1.0;
        }
        Object value = EventFieldResolver.resolve(input.event(), input.definition().weightField())
                .orElseThrow(() -> new IllegalArgumentException(
                        "missing weightField " + input.definition().weightField()));
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        throw new IllegalArgumentException("weightField must resolve to a number");
    }
}
