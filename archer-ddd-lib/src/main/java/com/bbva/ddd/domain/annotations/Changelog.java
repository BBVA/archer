package com.bbva.ddd.domain.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Changelog handler method annotation. For example:
 * <pre>
 *  {@code
 *   &#64;Changelog("base")
 *   public void processDataChangelog(ChangelogRecord changelog) {
 *      //Manage new changelog
 *   }
 *  }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Changelog {

    /**
     * Base name of changelog
     *
     * @return the base name
     */
    String value();

}
