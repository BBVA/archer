package com.bbva.dataprocessors.util;

import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecord;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

public final class ObjectUtils {

    private static final Logger logger = LoggerFactory.getLogger(ObjectUtils.class);

    public static Object merge(final Object lastObject, final Object newObject) {
        Object result;
        try {
            result = newObject.getClass().newInstance();

            try {
                for (final PropertyDescriptor pd : Introspector.getBeanInfo(result.getClass()).getPropertyDescriptors()) {
                    if (pd.getReadMethod() != null && !"class".equals(pd.getName()) && !"schema".equals(pd.getName())) {
                        Object newValue = null;
                        Object lastValue = null;

                        try {
                            newValue = pd.getReadMethod().invoke(newObject);
                            lastValue = pd.getReadMethod().invoke(lastObject);
                        } catch (final NullPointerException | IllegalAccessException | InvocationTargetException e) {
                            logger.error("Error invoking method", e);
                        }

                        try {
                            if (newValue == null) {
                                pd.getWriteMethod().invoke(result, lastValue);
                            } else if (newValue instanceof SpecificRecord) {
                                pd.getWriteMethod().invoke(result, merge(lastValue, newValue));
                            } else {
                                pd.getWriteMethod().invoke(result, newValue);
                            }
                        } catch (final NullPointerException | IllegalAccessException | InvocationTargetException e) {
                            logger.error("Error invoking method", e);
                        }
                    }
                }
            } catch (final IntrospectionException e) {
                logger.error("Introspect exception", e);
            }
        } catch (final InstantiationException | IllegalAccessException e) {
            logger.error("Error creating", e);
            result = lastObject;
        }

        return result;
    }
}
