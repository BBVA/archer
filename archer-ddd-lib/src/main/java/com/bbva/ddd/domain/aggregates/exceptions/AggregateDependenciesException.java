package com.bbva.ddd.domain.aggregates.exceptions;

public class AggregateDependenciesException extends Throwable {

    private static final long serialVersionUID = 6231818687606572725L;

    public AggregateDependenciesException() {
        super();
    }

    public AggregateDependenciesException(final String message) {
        super(message);
    }

    public AggregateDependenciesException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public AggregateDependenciesException(final Throwable cause) {
        super(cause);
    }
}
