/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.math.stats;

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
   * Observes the given sample <code>value</code>.
   */
  public abstract void observe(double value);
  
  /**
   * Clears the sampler's state.
   */
  public abstract void clear();

}
