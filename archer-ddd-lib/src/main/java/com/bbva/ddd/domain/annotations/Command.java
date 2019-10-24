package com.bbva.ddd.domain.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Command handler method annotation. For example:
 * <pre>
 *  {@code
 *   &#64;Command(commandAction = "action", source = "base")
 *   public void processCommand(CommandRecord command) {
 *      //Manage new command
 *   }
 *  }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Command {

    /**
     * Base name of command
     *
     * @return the base name
     */
    String source();

    /**
     * Command action
     *
     * @return the action
     */
    String action();
}
