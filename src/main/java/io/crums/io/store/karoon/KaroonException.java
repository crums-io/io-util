/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.karoon;

import io.crums.io.IoStateException;

/**
 * 
 * @author Babak
 */
public class KaroonException extends IoStateException {

  private static final long serialVersionUID = 1L;


  public KaroonException() {
  }


  public KaroonException(String message) {
    super(message);
  }


  public KaroonException(Throwable cause) {
    super(cause);
  }


  public KaroonException(String message, Throwable cause) {
    super(message, cause);
  }

}
