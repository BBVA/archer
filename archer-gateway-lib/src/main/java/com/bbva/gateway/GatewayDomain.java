package com.bbva.gateway;

import com.bbva.ddd.domain.Domain;
import com.bbva.gateway.builders.dataflows.states.HeaderAsKeyStateBuilder;
import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.handlers.GatewayHandler;

import static com.bbva.gateway.constants.Constants.INTERNAL_SUFFIX;
import static com.bbva.gateway.constants.Constants.KEY_SUFFIX;


public class GatewayDomain extends Domain {
    private final Domain domain;

    private GatewayDomain(final Domain domain) {
        super(null, null);
        this.domain = domain;
    }

    public static class Builder extends Domain.Builder {
        private final GatewayConfig gatewayConfig;
        private static Builder instance;
        private String servicesPackage;

        private Builder(final GatewayConfig config) {
            super(config);
            gatewayConfig = config;
        }

        public static Builder create() {
            instance = new Builder(ConfigBuilder.create());
            return instance;
        }

        public static Builder create(final Class classType) {
            instance = new Builder(ConfigBuilder.create(classType));
            return instance;
        }

        public Builder servicesPackage(final String servicesPackage) {
            this.servicesPackage = servicesPackage;
            return this;
        }

        @Override
        public GatewayDomain build() {
            handler(new GatewayHandler(servicesPackage, gatewayConfig));

            addDataProcessorBuilder(INTERNAL_SUFFIX + KEY_SUFFIX, new HeaderAsKeyStateBuilder(INTERNAL_SUFFIX + KEY_SUFFIX, INTERNAL_SUFFIX));

            final GatewayDomain gatewayDomain = new GatewayDomain(super.build());
            return gatewayDomain;
        }
    }
}
