package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureValue;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
class GenericFeatureProcessor implements PlatformEventProcessor {

    private final GenericAggregationEngine engine;
    private final FeatureRedisMaterializer materializer;

    GenericFeatureProcessor(GenericAggregationEngine engine, FeatureRedisMaterializer materializer) {
        this.engine = engine;
        this.materializer = materializer;
    }

    @Override
    public boolean supports(PlatformEvent event) {
        return engine.supports(event);
    }

    @Override
    public FeatureValue process(PlatformEvent event) {
        return process(event, EventTimeAssessment.onTimeAt(event.eventTime()));
    }

    @Override
    public FeatureValue process(PlatformEvent event, EventTimeAssessment assessment) {
        List<AggregationResult> results = engine.process(event, assessment);
        for (AggregationResult result : results) {
            materializer.materialize(result);
        }
        return results.isEmpty() ? null : results.getFirst().featureValue();
    }
}
