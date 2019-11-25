package com.bbva.ddd.domain.changelogs.repository.aggregates.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Aggregate entity class annotation. For example:
 * <pre>
 *  {@code
 *   &#64;Aggregate("base_name")
 *   public class MyAggregate extends CommonAggregate {
 *      //manage new event
 *   }
 *  }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Aggregate {

    /**
     * Base name of event
     *
     * @return the base name
     */
    String value();

}
