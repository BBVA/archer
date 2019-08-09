package com.bbva.gateway.util;

import java.util.HashSet;
import java.util.Set;

public class PowermockState {

    private ClassLoader origClassLoader;

    private final Set<Class<?>> classesToPrepare = new HashSet<>();

    private final Set<String> packagesToIgnore = new HashSet<>();

    public ClassLoader getOrigClassLoader() {
        return origClassLoader;
    }

    void setOrigClassLoader(final ClassLoader origClassLoader) {
        this.origClassLoader = origClassLoader;
    }

    public String[] getClassesToPrepareAsString() {
        final String[] result = new String[classesToPrepare.size()];
        final Class[] classes = classesToPrepare.toArray(new Class[classesToPrepare.size()]);
        for (int i = 0; i < result.length; i++) {
            result[i] = classes[i].getName();
        }
        return result;
    }

    public String[] getPackagesToIgnoreAsArray() {
        final String[] result = packagesToIgnore.toArray(new String[packagesToIgnore.size()]);
        return result;
    }

}
