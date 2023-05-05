package io.burt.athena;

import java.sql.SQLException;

public class SQLAlreadyClosedException extends SQLException {
    public SQLAlreadyClosedException(String name) {
        super(name + " has already been closed.");
    }
}