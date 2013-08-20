/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store;


import com.gnahraf.io.IoStateException;

/**
 * Thrown on encountering an unsorted element in what's supposed to be a
 * sorted structure.
 * 
 * @author Babak
 */
public class NotSortedException extends IoStateException {

  private static final long serialVersionUID = 1L;


  public NotSortedException() {
  }


  public NotSortedException(String message) {
    super(message);
  }


  public NotSortedException(Throwable cause) {
    super(cause);
  }


  public NotSortedException(String message, Throwable cause) {
    super(message, cause);
  }

}
