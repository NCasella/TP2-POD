package ar.edu.itba.pod.exceptions;

public class InvalidParamException extends RuntimeException {
    private final String MESSAGE;
    public InvalidParamException(final String message) {
        super(message);
        this.MESSAGE = message;
    }
    public String getMessage() {
        return MESSAGE;
    }
}
