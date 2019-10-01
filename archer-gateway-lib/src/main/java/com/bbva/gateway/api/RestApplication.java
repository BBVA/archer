package com.bbva.gateway.api;

import javax.ws.rs.core.Application;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Class to manage rest services of the application
 */
public class RestApplication extends Application {
    private final Set<Class<?>> classes;

    /**
     * Constructor
     */
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
