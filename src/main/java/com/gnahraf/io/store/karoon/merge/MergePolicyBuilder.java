/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;

/**
 * 
 * @author Babak
 */
public class MergePolicyBuilder extends MergePolicy {
  
  private int writeAheadFlushTrigger = 8 * 1024;
  private int youngThreshold = 2;
  private int generationalFactor = 6;
  private int maxMergeThreads = 3;
  

  
  
  public void setWriteAheadFlushTrigger(int bytes) {
    this.writeAheadFlushTrigger = bytes;
  }

  
  @Override
  public int getWriteAheadFlushTrigger() {
    return writeAheadFlushTrigger;
  }
  

  public void setYoungThreshold(int youngThreshold) {
    this.youngThreshold = youngThreshold;
  }

  @Override
  public int getMinYoungMergeTableCount() {
    return youngThreshold;
  }



  public void setGenerationalFactor(int factor) {
    if (factor < 2)
      throw new IllegalArgumentException("factor: " + factor);
    this.generationalFactor = factor;
  }

  @Override
  public int getGenerationalFactor() {
    return generationalFactor;
  }


  @Override
  public int getMaxMergeThreads() {
    return maxMergeThreads;
  }


  public void setMaxMergeThreads(int maxMergeThreads) {
    if (maxMergeThreads < 1)
      throw new IllegalArgumentException("maxMergeThreads: " + maxMergeThreads);
    this.maxMergeThreads = maxMergeThreads;
  }


  public MergePolicy snapshot() {
    final int waft = getWriteAheadFlushTrigger();
    final int yt = getMinYoungMergeTableCount();
    final int gf = getGenerationalFactor();
    final int mt = getMaxMergeThreads();
    return
        new MergePolicy() {
          @Override
          public int getWriteAheadFlushTrigger() {
            return waft;
          }
          @Override
          public int getMinYoungMergeTableCount() {
            return yt;
          }
          @Override
          public int getGenerationalFactor() {
            return gf;
          }
          @Override
          public int getMaxMergeThreads() {
            return mt;
          }
          @Override
          public String toString() {
            return "[waft=" + waft + ", yt=" + yt + ", gf=" + gf + "]";
          }
        };
  }

  @Override
  public String toString() {
    return snapshot().toString();
  }

}
