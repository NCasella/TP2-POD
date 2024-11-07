package ar.edu.itba.pod.exceptions;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

public class InvalidFilePathException extends NoSuchFileException {
    public InvalidFilePathException(final String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        return "Invalid path " + super.getMessage();
    }
}
