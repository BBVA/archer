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
    protected static String servicesPackage;
    protected static Domain domain;


    public static void main(final String[] args) throws RepositoryException {
        configure();

        domain = new Domain(
                new GatewayHandler(servicesPackage, config),
                config.getApplicationConfig());

        start();
    }

    protected static void configure(){
        final Config configAnnotation = Configuration.findConfigAnnotation();
        config = new Configuration().init(configAnnotation);
        servicesPackage = configAnnotation.servicesPackage();
    }

    protected static void start(){
        DataProcessor.get()
                .add(INTERNAL_SUFFIX + KEY_SUFFIX, new ChangelogKeyBuilder(INTERNAL_SUFFIX + KEY_SUFFIX, INTERNAL_SUFFIX));

        domain.start();
    }
}
