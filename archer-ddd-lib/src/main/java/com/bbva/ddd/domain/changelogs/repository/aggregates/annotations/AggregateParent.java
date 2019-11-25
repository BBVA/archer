package com.bbva.ddd.domain.changelogs.repository.aggregates.annotations;

import com.bbva.ddd.domain.changelogs.repository.aggregates.AbstractAggregate;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Parent aggregate entity class annotation. For example:
 * <pre>
 *  {@code
 *   &#64;AggregateParent(AggregateChild.class)
 *   public class MyAggregate extends CommonAggregate {
 *      //manage new event
 *   }
 *  }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface AggregateParent {
    /**
     * Aggregate child class
     *
     * @return the class
     */
    Class<? extends AbstractAggregate> value();
}
