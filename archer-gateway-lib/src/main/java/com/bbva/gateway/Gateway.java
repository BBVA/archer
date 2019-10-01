package com.bbva.gateway;

import com.bbva.dataprocessors.DataProcessor;
import com.bbva.ddd.domain.Domain;
import com.bbva.ddd.domain.DomainBuilder;
import com.bbva.ddd.domain.changelogs.exceptions.RepositoryException;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.consumer.builder.ChangelogKeyBuilder;

import static com.bbva.gateway.constants.Constants.INTERNAL_SUFFIX;
import static com.bbva.gateway.constants.Constants.KEY_SUFFIX;

/**
 * Main class to init gateway with your services
 * For example:
 * <pre>
 * {@code
 *  @Config(file = "example.yml", servicesPackage = "com.example")
 *  class ExampleGateway extends Gateway {
 *      ExampleGateway() {
 *          configure(ExampleGateway.class);
 *          domain = new Domain(new ExampleHandler(servicesPackage, config), config.getApplicationConfig());
 *          start();
 *          new HelperApplication(config.getApplicationConfig());
 *      }
 *  }
 * }</pre>
 */
public class Gateway {
    protected static Configuration config;
    protected static String servicesPackage;
    protected static Domain domain;

    /**
     * Initialize the domain of the gateway
     *
     * @throws RepositoryException exception
     */
    public void init() throws RepositoryException {
        configure();

        domain = DomainBuilder.create(
                new GatewayHandler(servicesPackage, config),
                config.getApplicationConfig());
    }

    /**
     * Configure the gateway
     *
     * @param classType class to obtain base package
     */
    protected void configure(final Class classType) {
        final Config configAnnotation = (Config) classType.getAnnotation(Config.class);
        config = new Configuration().init(configAnnotation);
        servicesPackage = configAnnotation.servicesPackage();
    }

    /**
     * Configure the gateway
     */
    protected void configure() {
        final Config configAnnotation = Configuration.findConfigAnnotation();
        config = new Configuration().init(configAnnotation);
        servicesPackage = configAnnotation.servicesPackage();
    }

    /**
     * Start domain and processors
     */
    protected void start() {
        DataProcessor.get()
                .add(INTERNAL_SUFFIX + KEY_SUFFIX, new ChangelogKeyBuilder(INTERNAL_SUFFIX + KEY_SUFFIX, INTERNAL_SUFFIX));

        domain.start();
    }
}
