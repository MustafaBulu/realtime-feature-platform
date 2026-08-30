package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureValue;

interface PlatformEventProcessor {

    boolean supports(PlatformEvent event);

    FeatureValue process(PlatformEvent event);
}
