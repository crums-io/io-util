/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.math.stats;

/**
 * Write-interface for sampling numeric values.
 * 
 * 
 * @author Babak
 */
public abstract class Sampler {
  
  
  public void observe(long value) {
    observe((double) value);
  }
  
  /**
   * Observes the given sample <tt>value</tt>.
   */
  public abstract void observe(double value);
  
  /**
   * Clears the sampler's state.
   */
  public abstract void clear();

}
