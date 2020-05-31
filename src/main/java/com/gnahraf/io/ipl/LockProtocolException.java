/*
 * Copyright 2020 Babak Farhang
 */
package com.gnahraf.io.ipl;

/**
 * Indicates someone else seems not to have played by the rules.
 */
public class LockProtocolException extends IllegalStateException {

  /**
   * 
   */
  public LockProtocolException() {
  }

  /**
   * @param s
   */
  public LockProtocolException(String s) {
    super(s);
  }

  /**
   * @param cause
   */
  public LockProtocolException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public LockProtocolException(String message, Throwable cause) {
    super(message, cause);
  }

}
