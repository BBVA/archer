package com.bbva.gateway;

import com.bbva.dataprocessors.DataProcessor;
import com.bbva.ddd.domain.Domain;
import com.bbva.ddd.domain.changelogs.exceptions.RepositoryException;
import com.bbva.gateway.config.Config;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.consumer.builder.ChangelogKeyBuilder;

import static com.bbva.gateway.constants.Constants.INTERNAL_SUFFIX;
import static com.bbva.gateway.constants.Constants.KEY_SUFFIX;

public abstract class Gateway {
    protected static Configuration config;

    public static void main(final String[] args) throws RepositoryException {
        final Config configAnnotation = Configuration.findConfigAnnotation();
        config = new Configuration().init(configAnnotation);

        final Domain domain = new Domain(
                new GatewayHandler(configAnnotation.servicesPackage(), config),
                config.getApplicationConfig());

        DataProcessor.get()
                .add(INTERNAL_SUFFIX + KEY_SUFFIX, new ChangelogKeyBuilder(INTERNAL_SUFFIX + KEY_SUFFIX, INTERNAL_SUFFIX));
        domain.start();
    }
}
