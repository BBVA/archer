package com.bbva.common.util;

import org.junit.gen5.api.extension.*;
import org.junit.gen5.api.extension.ExtensionContext.Namespace;
import org.junit.gen5.api.extension.ExtensionContext.Store;
import org.powermock.core.MockRepository;
import org.powermock.core.classloader.MockClassLoader;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.core.classloader.annotations.UseClassPathAdjuster;
import org.powermock.core.spi.PowerMockPolicy;
import org.powermock.core.transformers.MockTransformer;
import org.powermock.core.transformers.impl.MainMockTransformer;
import org.powermock.reflect.Whitebox;
import org.powermock.reflect.proxyframework.RegisterProxyFramework;
import org.powermock.tests.utils.TestClassesExtractor;
import org.powermock.tests.utils.impl.ArrayMergerImpl;
import org.powermock.tests.utils.impl.MockPolicyInitializerImpl;
import org.powermock.tests.utils.impl.PowerMockIgnorePackagesExtractorImpl;
import org.powermock.tests.utils.impl.PrepareForTestExtractorImpl;

import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PowermockExtension implements InstancePostProcessor, AfterEachExtensionPoint, BeforeEachExtensionPoint, BeforeAllExtensionPoint {
    @Override
    public void beforeEach(final TestExtensionContext context) {
        final PowermockState state = getState(context);
        state.setOrigClassLoader(Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(context.getTestClass().getClassLoader());
        new MockPolicyInitializerImpl(context.getTestClass()).initialize(context.getTestClass().getClassLoader());
    }

    @Override
    public void afterEach(final TestExtensionContext context) {
        final PowermockState state = getState(context);
        if (state.getOrigClassLoader() != null) {
            Thread.currentThread().setContextClassLoader(state.getOrigClassLoader());
            state.setOrigClassLoader(null);
        }
    }

    @Override
    public void beforeAll(final ContainerExtensionContext context) {
        MockRepository.clear();
    }

    public static PowermockState getState(final ExtensionContext context) {
        final Store store = context.getStore(Namespace.of(PowermockExtension.class));
        return (PowermockState) store.getOrComputeIfAbsent(PowermockState.class, (type) -> new PowermockState());
    }

    @Override
    public void postProcessTestInstance(final TestExtensionContext context) throws Exception {
        final PowermockState state = getState(context);
        final Class<?> origClass = context.getTestClass();
        final Method origMethod = context.getTestMethod();

        final TestClassesExtractor prepareForTestExtractor = new PrepareForTestExtractorImpl();
        final String[] prepareForTestClasses = prepareForTestExtractor.getTestClasses(origClass);
        final String[] stateTestClasses = state.getClassesToPrepareAsString();
        final String[] stateIgnorePackages = state.getPackagesToIgnoreAsArray();
        final String[] ignorePackages = new PowerMockIgnorePackagesExtractorImpl().getPackagesToIgnore(origClass);
        final ClassLoader defaultMockLoader = createNewClassloader(
                origClass,
                new ArrayMergerImpl().mergeArrays(String.class, prepareForTestClasses, stateTestClasses),
                new ArrayMergerImpl().mergeArrays(String.class, ignorePackages, stateIgnorePackages)
        );

        registerProxyframework(defaultMockLoader);

        final Class<?> newTestClass = defaultMockLoader.loadClass(origClass.getName());
        Method newTestMethod = null;
        final String origSignature = origMethod.toGenericString();
        for (final Method mth : newTestClass.getDeclaredMethods()) {
            if (mth.getName().equals(origMethod.getName()) && mth.toGenericString().equals(origSignature)) {
                newTestMethod = mth;
                break;
            }
        }
        if (newTestMethod == null) {
            throw new IllegalStateException();
        }

        final Object testDescriptor = Whitebox.getInternalState(context, "testDescriptor");
        Whitebox.setInternalState(context, "testInstance", newTestClass.newInstance());
        Whitebox.setInternalState(testDescriptor, "testClass", (Object) newTestClass);
        Whitebox.setInternalState(testDescriptor, "testMethod", newTestMethod);
    }


    private boolean hasMockPolicyProvidedClasses(final Class<?> testClass) {
        boolean hasMockPolicyProvidedClasses = false;
        if (testClass.isAnnotationPresent(MockPolicy.class)) {
            final MockPolicy annotation = testClass.getAnnotation(MockPolicy.class);
            final Class<? extends PowerMockPolicy>[] value = annotation.value();
            hasMockPolicyProvidedClasses = new MockPolicyInitializerImpl(value).needsInitialization();
        }
        return hasMockPolicyProvidedClasses;
    }

    private void registerProxyframework(final ClassLoader classLoader) {
        final Class<?> proxyFrameworkClass;
        try {
            proxyFrameworkClass = Class.forName("org.powermock.api.extension.proxyframework.ProxyFrameworkImpl", false, classLoader);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Extension API internal error: org.powermock.api.extension.proxyframework.ProxyFrameworkImpl could not be located in classpath.", e);
        }

        final Class<?> proxyFrameworkRegistrar;
        try {
            proxyFrameworkRegistrar = Class.forName(RegisterProxyFramework.class.getName(), false, classLoader);
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            Whitebox.invokeMethod(proxyFrameworkRegistrar, "registerProxyFramework", Whitebox.newInstance(proxyFrameworkClass));
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ClassLoader createNewClassloader(
            final Class<?> testClass,
            final String[] preliminaryClassesToLoadByMockClassloader,
            final String[] packagesToIgnore,
            final MockTransformer... extraMockTransformers) {
        final ClassLoader mockLoader;
        final String[] classesToLoadByMockClassloader = makeSureArrayContainsTestClassName(
                preliminaryClassesToLoadByMockClassloader, testClass.getName());
        if ((classesToLoadByMockClassloader == null || classesToLoadByMockClassloader.length == 0) && !hasMockPolicyProvidedClasses(testClass)) {
            mockLoader = Thread.currentThread().getContextClassLoader();
        } else {
            final List<MockTransformer> mockTransformerChain = new ArrayList<>();
            final MainMockTransformer mainMockTransformer = new MainMockTransformer();
            mockTransformerChain.add(mainMockTransformer);
            Collections.addAll(mockTransformerChain, extraMockTransformers);
            final UseClassPathAdjuster useClassPathAdjuster = testClass.getAnnotation(UseClassPathAdjuster.class);
            mockLoader = AccessController.doPrivileged((PrivilegedAction<MockClassLoader>) () -> new MockClassLoader(classesToLoadByMockClassloader, packagesToIgnore, useClassPathAdjuster));
            final MockClassLoader mockClassLoader = (MockClassLoader) mockLoader;
            mockClassLoader.setMockTransformerChain(mockTransformerChain);
            new MockPolicyInitializerImpl(testClass).initialize(mockLoader);
        }
        return mockLoader;
    }

    private String[] makeSureArrayContainsTestClassName(
            final String[] arrayOfClassNames, final String testClassName) {
        if (null == arrayOfClassNames || 0 == arrayOfClassNames.length) {
            return new String[]{testClassName};
        } else {
            final List<String> modifiedArrayOfClassNames = new ArrayList<>(
                    arrayOfClassNames.length + 1);
            modifiedArrayOfClassNames.add(testClassName);
            for (final String className : arrayOfClassNames) {
                if (testClassName.equals(className)) {
                    return arrayOfClassNames;
                } else {
                    modifiedArrayOfClassNames.add(className);
                }
            }
            return modifiedArrayOfClassNames.toArray(
                    new String[arrayOfClassNames.length + 1]);
        }
    }

}
