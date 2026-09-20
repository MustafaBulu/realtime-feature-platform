package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import org.springframework.stereotype.Component;

@Component
class SumAggregator implements Aggregator {

    @Override
    public AggregationType type() {
        return AggregationType.SUM;
    }

    @Override
    public Number aggregate(AggregationInput input) {
        double increment = numericField(input).doubleValue();
        double next = NumericStateFormat.readDouble(input.stateStore().get(input.stateKey())) + increment;
        input.stateStore().put(input.stateKey(), NumericStateFormat.writeNumber(next));
        return NumericStateFormat.asNumber(next);
    }

    private static Number numericField(AggregationInput input) {
        Object value = EventFieldResolver.resolve(input.event(), input.definition().valueField())
                .orElseThrow(() -> new IllegalArgumentException(
                        "missing valueField " + input.definition().valueField()));
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalArgumentException("valueField must resolve to a number");
    }
}
