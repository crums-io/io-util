/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io;


/**
 * Thrown on unexpected end of stream.
 * 
 * @author Babak
 */
public class EofException extends IoStateException {

  private static final long serialVersionUID = 1L;

  public EofException() {
  }


  public EofException(String message) {
    super(message);
  }


  public EofException(Throwable cause) {
    super(cause);
  }


  public EofException(String message, Throwable cause) {
    super(message, cause);
  }

}
