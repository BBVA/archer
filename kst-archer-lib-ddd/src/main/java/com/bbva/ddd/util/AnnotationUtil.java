package com.bbva.ddd.util;

import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

public final class AnnotationUtil {

    public static <C extends Annotation> List<Class> findAllAnnotatedClasses(final Class<C> annotation) {
        final String mainPackage = AnnotationUtil.class.getCanonicalName().split("\\.")[0];
        List<Class> handlers = findHandlers(mainPackage, annotation);
        if (handlers.isEmpty()) {
            handlers = findAllHandlers(annotation);
        }
        return handlers;
    }

    private static <C extends Annotation> List<Class> findAllHandlers(final Class<C> annotation) {
        final List<Class> handlers = new ArrayList<>();
        final Package[] packages = Package.getPackages();
        for (final Package packageLoaded : packages) {
            if (!packageLoaded.getName().matches("^(org|sun|java|jdk).*")) {
                handlers.addAll(findHandlers(packageLoaded.getName(), annotation));
            }
        }
        return handlers;
    }

    private static <C extends Annotation> List<Class> findHandlers(final String mainPackage, final Class<C> annotation) {
        final List<Class> handlers = new ArrayList<>();
        final Reflections ref = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(mainPackage, ClasspathHelper.contextClassLoader(),
                        ClasspathHelper.staticClassLoader()))
                .filterInputsBy(new FilterBuilder().include(".+\\.class")));

        for (final Class<?> aggregateClass : ref.getTypesAnnotatedWith(annotation)) {
            handlers.add(aggregateClass);
        }
        return handlers;
    }

}
