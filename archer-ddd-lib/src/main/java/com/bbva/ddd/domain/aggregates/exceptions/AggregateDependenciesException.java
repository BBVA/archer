package com.bbva.ddd.domain.aggregates.exceptions;

/**
 * Specific aggregate with dependencies exception
 */
public class AggregateDependenciesException extends Throwable {

    private static final long serialVersionUID = 6231818687606572725L;

    /**
     * Constructor
     */
    public AggregateDependenciesException() {
        super();
    }

    /**
     * Constructor
     *
     * @param message error message
     */
    public AggregateDependenciesException(final String message) {
        super(message);
    }

    /**
     * Constructor
     *
     * @param message error message
     * @param cause   exception cause
     */
    public AggregateDependenciesException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor
     *
     * @param cause exception cause
     */
    public AggregateDependenciesException(final Throwable cause) {
        super(cause);
    }
}
