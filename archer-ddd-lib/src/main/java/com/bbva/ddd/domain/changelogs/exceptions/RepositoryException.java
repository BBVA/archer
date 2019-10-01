package com.bbva.ddd.domain.changelogs.exceptions;

/**
 * Custom exception indicate repository problems
 */
public class RepositoryException extends RuntimeException {

    private static final long serialVersionUID = 2554593551091176856L;

    /**
     * Constructor
     */
    public RepositoryException() {
        super();
    }

    /**
     * Constructor
     *
     * @param message error message
     */
    public RepositoryException(final String message) {
        super(message);
    }

    /**
     * Constructor
     *
     * @param message error message
     * @param cause   exception cause
     */
    public RepositoryException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor
     *
     * @param cause exception cause
     */
    public RepositoryException(final Throwable cause) {
        super(cause);
    }
}
