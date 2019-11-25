package com.bbva.ddd.domain.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Event handler method annotation. For example:
 * <pre>
 *  {@code
 *   &#64;Event("base")
 *   public void processEvent(EventRecord event) {
 *      //manage new event
 *   }
 *  }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Event {

    /**
     * Base name of event
     *
     * @return the base name
     */
    String value();

}
