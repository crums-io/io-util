/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;

/**
 * 
 * @author Babak
 */
public class MergePolicyBuilder implements MergePolicy {
  
  private int writeAheadFlushTrigger = 8 * 1024;
  private int youngThreshold = 3;
  private long maxYoungSize = writeAheadFlushTrigger * 6;
  private long mediumMaxSize = 1024 * 1024;
  

  
  
  public void setWriteAheadFlushTrigger(int bytes) {
    this.writeAheadFlushTrigger = bytes;
    if (bytes > maxYoungSize)
      maxYoungSize = bytes;
  }

  
  @Override
  public int getWriteAheadFlushTrigger() {
    return writeAheadFlushTrigger;
  }
  

  public void setYoungThreshold(int youngThreshold) {
    this.youngThreshold = youngThreshold;
  }

  @Override
  public int getYoungThreshold() {
    return youngThreshold;
  }


  public void setMaxYoungSize(long maxYoungSize) {
    this.maxYoungSize = maxYoungSize;
    if (maxYoungSize < writeAheadFlushTrigger)
      writeAheadFlushTrigger = (int) maxYoungSize;
  }
  
  @Override
  public long getMaxYoungSize() {
    return maxYoungSize;
  }



  public void setMediumMaxSize(long mediumMaxSize) {
    this.mediumMaxSize = mediumMaxSize;
  }

  @Override
  public long getMediumMaxSize() {
    return mediumMaxSize;
  }


  public MergePolicy snapshot() {
    final int waft = getWriteAheadFlushTrigger();
    final int yt = getYoungThreshold();
    final long ym = getMaxYoungSize();
    final long mm = getMediumMaxSize();
    return
        new MergePolicy() {
          @Override
          public int getWriteAheadFlushTrigger() {
            return waft;
          }
          @Override
          public int getYoungThreshold() {
            return yt;
          }
          @Override
          public long getMaxYoungSize() {
            return ym;
          }
          @Override
          public long getMediumMaxSize() {
            return mm;
          }
          @Override
          public String toString() {
            return "[waft=" + waft + ", yt=" + yt + ", ym=" + ym + ", mm=" + mm + "]";
          }
        };
  }

  @Override
  public String toString() {
    return snapshot().toString();
  }

}
