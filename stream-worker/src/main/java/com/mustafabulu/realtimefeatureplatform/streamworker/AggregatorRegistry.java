package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
class AggregatorRegistry {

    private final Map<AggregationType, Aggregator> aggregators;

    AggregatorRegistry(List<Aggregator> aggregators) {
        EnumMap<AggregationType, Aggregator> byType = new EnumMap<>(AggregationType.class);
        for (Aggregator aggregator : aggregators) {
            Aggregator previous = byType.put(aggregator.type(), aggregator);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate aggregator for " + aggregator.type());
            }
        }
        this.aggregators = Map.copyOf(byType);
    }

    Aggregator get(AggregationType type) {
        Aggregator aggregator = aggregators.get(type);
        if (aggregator == null) {
            throw new IllegalArgumentException("unsupported aggregation type " + type);
        }
        return aggregator;
    }
}
