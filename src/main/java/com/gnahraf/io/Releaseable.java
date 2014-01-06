/*
 * Copyright 2014 Babak Farhang 
 */
package com.gnahraf.io;

import java.io.Closeable;

/**
 * 
 * @author Babak
 */
public interface Releaseable extends Closeable {
  
  /**
   * Releases (err, closes) the instance. No exceptions are thrown.
   * The idempotency requirement of the super interface must be observed.
   */
  void close();

}
