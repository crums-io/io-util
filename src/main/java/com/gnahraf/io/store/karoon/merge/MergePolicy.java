/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;

/**
 * 
 * @author Babak
 */
public abstract class MergePolicy {
  
  public abstract int getWriteAheadFlushTrigger();

  
  public abstract int getMinYoungMergeTableCount();

  /**
   * Returns the first generation's maximum table size.
   * 
   * @return <tt>getWriteAheadFlushTrigger() * getGenerationalFactor()</tt>
   */
  public long getMaxYoungSize() {
    return (long) (getWriteAheadFlushTrigger() * getGenerationalFactor());
  }
  
  /**
   * Returns the factor that determines each successive generation's maximum
   * table size.
   */
  public abstract double getGenerationalFactor();
  
  public long getGenerationMaxSize(int gen) {
    if (gen == 0)
      return getMaxYoungSize();
    double factor = getGenerationalFactor();
    return (long) (getMaxYoungSize() * Math.pow(factor, gen));
  }
  
  
  public abstract int getMaxMergeThreads();
  
  
  public abstract int getMergeThreadPriority();
  
  
  public abstract int getEngineOverheatTableCount();

}
