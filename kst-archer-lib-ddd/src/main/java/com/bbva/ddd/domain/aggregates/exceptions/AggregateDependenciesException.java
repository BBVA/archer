package com.bbva.ddd.domain.aggregates.exceptions;

public class AggregateDependenciesException extends Exception {

    public AggregateDependenciesException() {
        super();
    }

    public AggregateDependenciesException(String message) {
        super(message);
    }

    public AggregateDependenciesException(String message, Throwable cause) {
        super(message, cause);
    }

    public AggregateDependenciesException(Throwable cause) {
        super(cause);
    }
}
