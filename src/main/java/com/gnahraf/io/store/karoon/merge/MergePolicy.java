/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;

/**
 * 
 * @author Babak
 */
public interface MergePolicy {
  
  public int getWriteAheadFlushTrigger();

  public int getYoungThreshold();

  public long getMaxYoungSize();
  
  public long getMediumMaxSize();

}
