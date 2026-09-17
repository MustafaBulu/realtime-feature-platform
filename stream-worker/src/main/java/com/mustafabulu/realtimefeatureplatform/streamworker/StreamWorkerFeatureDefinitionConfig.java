package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.BuiltInFeatureDefinitions;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.InMemoryFeatureDefinitionRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class StreamWorkerFeatureDefinitionConfig {

    @Bean
    FeatureDefinitionRepository featureDefinitionRepository() {
        return new InMemoryFeatureDefinitionRepository(BuiltInFeatureDefinitions.all());
    }
}
