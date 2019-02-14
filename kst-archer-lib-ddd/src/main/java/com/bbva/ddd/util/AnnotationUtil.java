package com.bbva.ddd.util;

import com.bbva.ddd.domain.Handler;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

public final class AnnotationUtil {

    public static <C extends Annotation> List<Class> findAllAnnotatedClasses(final Class<C> annotation, final Handler handler) {
        final String mainPackage = handler.getClass().getCanonicalName().split("\\.")[0];
        return findAnnotations(findClassesInPackage(mainPackage, annotation), findInAllPackages(annotation));
    }

    public static <C extends Annotation> List<Class> findAllAnnotatedClasses(final Class<C> annotation) {
        final String mainPackage = AnnotationUtil.class.getCanonicalName().split("\\.")[0];
        return findAnnotations(findClassesInPackage(mainPackage, annotation), findInAllPackages(annotation));
    }

    private static List<Class> findAnnotations(final List<Class> classesInPackage, final List<Class> inAllPackages) {
        List<Class> handlers = classesInPackage;
        if (handlers.isEmpty()) {
            handlers = inAllPackages;
        }
        return handlers;
    }

    private static <C extends Annotation> List<Class> findInAllPackages(final Class<C> annotation) {
        final List<Class> handlers = new ArrayList<>();
        final Package[] packages = Package.getPackages();
        for (final Package packageLoaded : packages) {
            if (!packageLoaded.getName().matches("^(org|sun|java|jdk).*")) {
                handlers.addAll(findClassesInPackage(packageLoaded.getName(), annotation));
            }
        }
        return handlers;
    }

    private static <C extends Annotation> List<Class> findClassesInPackage(final String packageToFind, final Class<C> annotation) {
        final List<Class> handlers = new ArrayList<>();
        final Reflections ref = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(packageToFind, ClasspathHelper.contextClassLoader(),
                        ClasspathHelper.staticClassLoader()))
                .filterInputsBy(new FilterBuilder().include(".+\\.class")));

        for (final Class<?> aggregateClass : ref.getTypesAnnotatedWith(annotation)) {
            handlers.add(aggregateClass);
        }
        return handlers;
    }

}
