/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;

import com.gnahraf.io.IoStateException;

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
