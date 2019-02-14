package com.bbva.gateway;

import com.bbva.dataprocessors.DataProcessor;
import com.bbva.ddd.domain.Domain;
import com.bbva.ddd.domain.changelogs.exceptions.RepositoryException;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.consumer.builder.ChangelogKeyBuilder;

import static com.bbva.gateway.constants.Constants.INTERNAL_SUFFIX;
import static com.bbva.gateway.constants.Constants.KEY_SUFFIX;

public class Gateway {
    protected static Configuration config;
    protected static String servicesPackage;
    protected static Domain domain;

    public void init() throws RepositoryException {
        configure();

        domain = new Domain(
                new GatewayHandler(servicesPackage, config),
                config.getApplicationConfig());

    }

    protected void configure(final Class classType) {
        final Config configAnnotation = (Config) classType.getAnnotation(Config.class);
        config = new Configuration().init(configAnnotation);
        servicesPackage = configAnnotation.servicesPackage();
    }

    protected void configure() {
        final Config configAnnotation = Configuration.findConfigAnnotation();
        config = new Configuration().init(configAnnotation);
        servicesPackage = configAnnotation.servicesPackage();
    }

    protected void start() {
        DataProcessor.get()
                .add(INTERNAL_SUFFIX + KEY_SUFFIX, new ChangelogKeyBuilder(INTERNAL_SUFFIX + KEY_SUFFIX, INTERNAL_SUFFIX));

        domain.start();
    }
}
