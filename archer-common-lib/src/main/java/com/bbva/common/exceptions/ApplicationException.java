package com.bbva.common.exceptions;

public class ApplicationException extends RuntimeException {

    private static final long serialVersionUID = -8250375143559017596L;

    public ApplicationException() {
        super();
    }

    public ApplicationException(final String message) {
        super(message);
    }


    public ApplicationException(final String message, final Throwable cause) {
        super(message, cause);
    }


    public ApplicationException(final Throwable cause) {
        super(cause);
    }

}
