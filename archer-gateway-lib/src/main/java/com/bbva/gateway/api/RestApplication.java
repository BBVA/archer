package com.bbva.gateway.api;

import javax.ws.rs.core.Application;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RestApplication extends Application {
    private final Set<Class<?>> classes;

    public RestApplication() {
        final HashSet<Class<?>> restServices = new HashSet<>();
        restServices.add(CallbackRest.class);
        classes = Collections.unmodifiableSet(restServices);
    }

    @Override
    public Set<Class<?>> getClasses() {
        return classes;
    }
}
