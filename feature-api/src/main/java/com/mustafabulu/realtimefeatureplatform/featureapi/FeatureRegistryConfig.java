package com.mustafabulu.realtimefeatureplatform.featureapi;

import com.mustafabulu.realtimefeatureplatform.featuremodel.BuiltInFeatureDefinitions;
import com.mustafabulu.realtimefeatureplatform.featuremodel.MutableFeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.MutableInMemoryFeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featureregistry.JdbcFeatureDefinitionRepository;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
class FeatureRegistryConfig {

    @Bean
    @ConditionalOnProperty(name = "rfp.feature-registry.store", havingValue = "postgres")
    DataSource featureRegistryDataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password
    ) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(url, username, password);
        dataSource.setDriverClassName("org.postgresql.Driver");
        return dataSource;
    }

    @Bean
    @ConditionalOnProperty(name = "rfp.feature-registry.store", havingValue = "postgres")
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    @ConditionalOnProperty(name = "rfp.feature-registry.store", havingValue = "postgres")
    TransactionTemplate transactionTemplate(DataSource dataSource) {
        return new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Bean
    @ConditionalOnProperty(name = "rfp.feature-registry.store", havingValue = "postgres")
    ApplicationRunner featureRegistryMigration(DataSource dataSource) {
        return args -> Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @Bean
    @ConditionalOnProperty(name = "rfp.feature-registry.store", havingValue = "postgres")
    MutableFeatureDefinitionRepository jdbcFeatureDefinitionRepository(
            JdbcTemplate jdbcTemplate,
            TransactionTemplate transactionTemplate
    ) {
        return new JdbcFeatureDefinitionRepository(jdbcTemplate, transactionTemplate);
    }

    @Bean
    @ConditionalOnMissingBean(MutableFeatureDefinitionRepository.class)
    MutableFeatureDefinitionRepository featureDefinitionRepository() {
        return new MutableInMemoryFeatureDefinitionRepository(BuiltInFeatureDefinitions.all());
    }
}
