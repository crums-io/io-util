/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.util.api;

/**
 * Indicates a {@linkplain Serial} implementation's static
 * {@code load(ByteBuffer in)} method encountered a
 * validation problem.
 */
@SuppressWarnings("serial")
public class SerialFormatException extends FactoryException {

  public SerialFormatException() {
  }

  public SerialFormatException(String message) {
    super(message);
  }

  public SerialFormatException(Throwable cause) {
    super(cause);
  }

  public SerialFormatException(String message, Throwable cause) {
    super(message, cause);
  }

  public SerialFormatException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }




}
