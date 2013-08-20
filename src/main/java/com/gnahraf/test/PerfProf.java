/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.test;

import com.gnahraf.math.stats.SimpleSampler;

/**
 * 
 * @author Babak
 */
public class PerfProf {
  
  private SimpleSampler timeStats = new SimpleSampler();
  
  private long lapStartTime;
  
  
  public void begin() {
    lapStartTime = System.nanoTime();
  }
  
  public void end() {
    long now = System.nanoTime();
    timeStats.observe(now - lapStartTime);
    lapStartTime = now;
  }
  
  
  public void clear() {
    timeStats.clear();
  }

  public int getCount() {
    return timeStats.getCount();
  }

  public double getMeanNanos() {
    return timeStats.getMean();
  }

  public double getMeanNanosSquare() {
    return timeStats.getMeanSquare();
  }

  public double getNansosSd() {
    return timeStats.getSd();
  }

  public double getMinNanos() {
    return timeStats.getMin();
  }

  public double getMaxNanos() {
    return timeStats.getMax();
  }

  public double getSumNanos() {
    return timeStats.sum();
  }

}
