/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;

/**
 * 
 * @author Babak
 */
public class MergePolicyBuilder extends MergePolicy {
  
  private int writeAheadFlushTrigger = 32 * 1024;
  private int youngThreshold = 2;
  private double generationalFactor = 2.6;
  private int maxMergeThreads = 8;
  private int mergeThreadPriority = 3;
  private int engineOverheatTableCount = 64;

  
  
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



  public void setGenerationalFactor(double factor) {
    if (factor < 2)
      throw new IllegalArgumentException("factor: " + factor);
    this.generationalFactor = factor;
  }

  @Override
  public double getGenerationalFactor() {
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


  @Override
  public int getMergeThreadPriority() {
    return mergeThreadPriority;
  }


  public void setMergeThreadPriority(int mergeThreadPriority) {
    this.mergeThreadPriority = mergeThreadPriority;
  }


  @Override
  public int getEngineOverheatTableCount() {
    return engineOverheatTableCount;
  }


  public MergePolicy snapshot() {
    final int waft = getWriteAheadFlushTrigger();
    final int yt = getMinYoungMergeTableCount();
    final double gf = getGenerationalFactor();
    final int mt = getMaxMergeThreads();
    final int mp = getMergeThreadPriority();
    final int oc = getEngineOverheatTableCount();
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
          public double getGenerationalFactor() {
            return gf;
          }
          @Override
          public int getMaxMergeThreads() {
            return mt;
          }
          @Override
          public int getMergeThreadPriority() {
            return mp;
          }
          @Override
          public int getEngineOverheatTableCount() {
            return oc;
          }
          @Override
          public String toString() {
            return
                "[waft=" + waft +
                ", yt=" + yt +
                ", gf=" + gf +
                ", mt=" + mt +
                ", mp=" + mp +
                ", oc=" + oc +
                "]";
          }
        };
  }

  @Override
  public String toString() {
    return snapshot().toString();
  }

}
