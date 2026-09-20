package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;

record AggregationInput(
        FeatureDefinition definition,
        PlatformEvent event,
        String stateKey,
        AggregationStateStore stateStore
) {
}
