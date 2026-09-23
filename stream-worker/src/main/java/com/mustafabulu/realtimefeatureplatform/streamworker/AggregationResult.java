package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureValue;
import java.time.Instant;

record AggregationResult(
        FeatureDefinition definition,
        FeatureKey featureKey,
        String materializedKey,
        Number value,
        Instant updatedAt
) {

    FeatureValue featureValue() {
        return new FeatureValue(featureKey, value, updatedAt);
    }
}
