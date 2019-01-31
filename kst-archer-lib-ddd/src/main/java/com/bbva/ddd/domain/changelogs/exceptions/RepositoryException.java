package com.bbva.ddd.domain.changelogs.exceptions;

public class RepositoryException extends Exception {
    
    private static final long serialVersionUID = 2554593551091176856L;

    public RepositoryException() {
        super();
    }

    public RepositoryException(final String message) {
        super(message);
    }

    public RepositoryException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RepositoryException(final Throwable cause) {
        super(cause);
    }
}
