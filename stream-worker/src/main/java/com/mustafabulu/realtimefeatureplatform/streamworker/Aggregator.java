package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;

interface Aggregator {

    AggregationType type();

    Number aggregate(AggregationInput input);
}
