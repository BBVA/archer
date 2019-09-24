package com.bbva.dataprocessors.util;

import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecord;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utility class to manage object and fields
 */
public final class ObjectUtils {

    private static final Logger logger = LoggerFactory.getLogger(ObjectUtils.class);

    /**
     * Merge two object properties in one new object
     *
     * @param lastObject old object
     * @param newObject  new object properties
     * @return merge object
     */
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

    /**
     * Return the merged object or the intial if it not exists previously
     *
     * @param oldValue old value
     * @param value    new value
     * @param <V>      Class type of value
     * @return merged object
     */
    public static <V> V getNewMergedValue(final V oldValue, final V value) {
        final V newValue;
        if (oldValue == null || value == null) {
            newValue = value;
        } else {
            newValue = (V) merge(oldValue, value);
        }
        return newValue;
    }

    /**
     * Obtain the getter or setter of a property
     *
     * @param fieldName field name
     * @param isGet     gettter/setter
     * @return the method
     */
    public static String getFieldNameMethod(final String fieldName, final boolean isGet) {
        return (isGet ? "get" : "set") +
                fieldName.substring(0, 1).toUpperCase() +
                fieldName.substring(1);
    }
}
