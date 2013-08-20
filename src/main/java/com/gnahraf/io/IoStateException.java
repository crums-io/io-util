/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io;

import java.io.IOException;

/**
 * Represents an illegal I/O state.
 * 
 * @author Babak
 */
public class IoStateException extends IOException {

  private static final long serialVersionUID = 1L;


  public IoStateException() {
  }


  public IoStateException(String message) {
    super(message);
  }


  public IoStateException(Throwable cause) {
    super(cause);
  }


  public IoStateException(String message, Throwable cause) {
    super(message, cause);
  }

}
