/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.karoon.merge;

/**
 * Indicates historically inconsistent data. It represents a bug higher up the thrower's call stack.
 * Used by {@linkplain GenerationInfo}.
 * 
 * @author Babak
 */
public class HistoryException extends IllegalArgumentException {

  private static final long serialVersionUID = 1L;


  public HistoryException() {
  }


  public HistoryException(String message) {
    super(message);
  }


  public HistoryException(Throwable cause) {
    super(cause);
  }


  public HistoryException(String message, Throwable cause) {
    super(message, cause);
  }

}
